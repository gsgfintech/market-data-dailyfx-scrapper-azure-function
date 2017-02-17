using AngleSharp.Parser.Html;
using Capital.GSG.FX.Data.Core.ContractData;
using Capital.GSG.FX.Data.Core.MarketData;
using Capital.GSG.FX.Utils.Core;
using Capital.GSG.FX.Monitoring.Server.Connector;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Net;

private const string DailyFX = "https://www.dailyfx.com";
private const string CalendarEndpoint = "/calendar";

private static TraceWriter logger;

public static void Run(TimerInfo myTimer, TraceWriter log)
{
    logger = log;

    logger.Info("Starting DailyFX Scrapper");

    List<FXEvent> events = ScrapFXEvents();

    if (!events.IsNullOrEmpty())
    {
        try
        {
            PostEventsToMonitoringBackendServer(events).Wait();
        }
        catch (Exception ex)
        {
            logger.Error($"Failed to add events to SQL Server: {ex.Message}");
        }
    }
    else
        logger.Error("Found no event to process. Will exit");
}

private static List<FXEvent> ScrapFXEvents()
{
    string source = LoadPage();

    if (!string.IsNullOrEmpty(source))
        return ParseEvents(source);
    else
        logger.Error("Failed to read the page");

    return null;
}

private static string LoadPage()
{
    try
    {
        logger.Info($"Loading DailyFX calendar from {DailyFX}{CalendarEndpoint}");

        RestClient client = new RestClient(DailyFX);

        var request = new RestRequest(CalendarEndpoint, Method.GET);

        IRestResponse response = client.Execute(request);

        if (response != null && response.StatusCode == HttpStatusCode.OK && !string.IsNullOrEmpty(response.Content))
        {
            logger.Info("Successfully loaded the page");
            return response.Content;
        }
        else
        {
            logger.Error($"Failed to load calendar page (response {response?.StatusCode})");
            return null;
        }
    }
    catch (Exception ex)
    {
        logger.Error("Caught exception in LoadPage()", ex);
        return null;
    }
}

private static List<FXEvent> ParseEvents(string source)
{
    try
    {
        logger.Info("Parsing events from the source");

        List<FXEvent> events = new List<FXEvent>();

        HtmlParser parser = new HtmlParser();

        var document = parser.Parse(source);

        var eventsSelector = document.QuerySelectorAll(".event");

        foreach (var eventData in eventsSelector)
        {
            try
            {
                string ccy = eventData.Attributes["data-category"].Value;
                string level = eventData.Attributes["data-importance"].Value;
                string id = eventData.Attributes["data-id"]?.Value?.Replace("ev", "") ?? null;

                var timestampSelector = eventData.QuerySelector($"#date{id}");
                DateTimeOffset timestamp = DateTimeOffset.Parse(timestampSelector.InnerHtml, null, System.Globalization.DateTimeStyles.AssumeUniversal);

                var titleSelector = eventData.QuerySelector($"#title{id}");
                var toRemoveSelector = titleSelector.QuerySelector("div");
                titleSelector.RemoveChild(toRemoveSelector);
                string title = titleSelector.InnerHtml;

                var cell = titleSelector.NextElementSibling; // nothing

                cell = cell.NextElementSibling; // actual
                string actual = !string.IsNullOrEmpty(cell?.TextContent) ? cell.TextContent : null;

                cell = cell.NextElementSibling; // forecast
                string forecast = !string.IsNullOrEmpty(cell?.TextContent) ? cell.TextContent : null;

                cell = cell.NextElementSibling; // previous
                string previous = !string.IsNullOrEmpty(cell?.TextContent) ? cell.TextContent : null;

                // explanations (optional)
                string explanation = null;

                var explanationSelector = document.QuerySelector($"#daily{id}");
                if (explanationSelector != null)
                {
                    var gsstxSelector = explanationSelector.QuerySelector(".gsstx");

                    if (gsstxSelector != null)
                    {
                        var linkSelector = gsstxSelector.QuerySelector("a");
                        if (linkSelector != null)
                            gsstxSelector.RemoveChild(linkSelector);
                        explanation = gsstxSelector.InnerHtml;
                    }
                }

                events.Add(new FXEvent()
                {
                    Actual = actual,
                    Currency = CurrencyUtils.GetFromStr(ccy),
                    EventId = Guid.NewGuid().ToString(),
                    Explanation = explanation,
                    Forecast = forecast,
                    Level = FXEventLevelUtils.GetFromStr(level),
                    Previous = previous,
                    Timestamp = timestamp,
                    Title = title
                });
            }
            catch (Exception ex)
            {
                logger.Error("Failed to parse event", ex);
            }
        }

        logger.Info($"Parsed {events.Count} events");

        return events;
    }
    catch (Exception ex)
    {
        logger.Error("Caught exception in ParseEvents()", ex);
        return null;
    }
}

private static async Task PostEventsToMonitoringBackendServer(List<FXEvent> events)
{
    try
    {
        events.Sort(new FXEventTimeComparer());

        DateTimeOffset min = events.Select(e => e.Timestamp).Min();
        DateTimeOffset max = events.Select(e => e.Timestamp).Max();

        //string clientId = ConfigurationManager.AppSettings["MonitorDaemon:ClientId"];
        //string appKey = ConfigurationManager.AppSettings["MonitorDaemon:AppKey"];

        //string backendAddress = ConfigurationManager.AppSettings["MonitorServerBackend:Address"];
        //string backendAppUri = ConfigurationManager.AppSettings["MonitorServerBackend:AppUri"];
        string clientId = "";
        string appKey = "";

        string backendAddress = "";
        string backendAppUri = "";

        BackendFXEventsConnector connector = (new MonitoringServerConnector(backendAddress, appKey, backendAddress, backendAppUri)).FXEventsConnector;

        List<FXEvent> existing = await connector.GetInTimeRange(min, max);

        if (existing.IsNullOrEmpty())
        {
            logger.Info("Found no existing events in DB. Will add the whole batch as new events");

            foreach (var fxEvent in events)
                await connector.AddOrUpdate(fxEvent);

            logger.Info($"Successfully processed {events.Count} events");
        }
        else
        {
            logger.Info($"Found {existing.Count} existing events in DB between {min} and {max}");

            List<FXEvent> toProcess = new List<FXEvent>();

            var toAdd = events.Except(existing, FXEventEqualityComparer.Instance);

            if (!toAdd.IsNullOrEmpty())
            {
                logger.Info($"Found {toAdd.Count()} new events that are not in DB. Will add them");

                foreach (var fxEvent in toAdd)
                    logger.Info($"\t{FormatFXEvent(fxEvent)}");

                toProcess.AddRange(toAdd);
            }

            var toCheck = events.Intersect(existing, FXEventEqualityComparer.Instance);

            if (!toCheck.IsNullOrEmpty())
            {
                logger.Info($"Found {toCheck.Count()} events already in DB. Will check them one by one");

                foreach (var fxEvent in toCheck)
                {
                    var updated = events.First(e => e.Timestamp == fxEvent.Timestamp && e.Title == fxEvent.Title && e.Currency == fxEvent.Currency);
                    var current = existing.First(e => e.Timestamp == fxEvent.Timestamp && e.Title == fxEvent.Title && e.Currency == fxEvent.Currency);

                    if (updated.Actual != current.Actual
                        || updated.Explanation != current.Explanation
                        || updated.Forecast != current.Forecast
                        || updated.Level != current.Level
                        || updated.Previous != current.Previous)
                    {
                        logger.Info($"Event {FormatFXEvent(fxEvent)} was updated on DailyFX and needs to be updated in the DB");
                        logger.Info($"\tPrev level: {current.Level}, New level: {updated.Level}");
                        logger.Info($"\tPrev explanation: {current.Explanation}, New explanation: {updated.Explanation}");
                        logger.Info($"\tPrev forecast: {current.Forecast}, New forecast: {updated.Forecast}");
                        logger.Info($"\tPrev previous: {current.Previous}, New previous: {updated.Previous}");
                        logger.Info($"\tPrev actual: {current.Actual}, New actual: {updated.Actual}");

                        toProcess.Add(fxEvent);
                    }
                }
            }

            if (!toProcess.IsNullOrEmpty())
            {
                foreach (var fxEvent in toProcess)
                    await connector.AddOrUpdate(fxEvent);

                logger.Info($"Successfully processed {toProcess.Count} events");
            }
            else
                logger.Info("All events received from DailyFX, but none was found to be new or updated. Nothing to add/update in DB");
        }
    }
    catch (Exception ex)
    {
        logger.Error($"Failed to process events: {ex}");
    }
}

private static string FormatFXEvent(FXEvent fxEvent)
{
    return $"{fxEvent.Timestamp:dd/MM/yy HH:mm:ss zzz} - {fxEvent.Currency} - {fxEvent.Title}";
}

private class FXEventTimeComparer : IComparer<FXEvent>
{
    public int Compare(FXEvent x, FXEvent y)
    {
        return x.Timestamp.CompareTo(y.Timestamp);
    }
}

private class FXEventEqualityComparer : IEqualityComparer<FXEvent>
{
    private static FXEventEqualityComparer instance;

    public static FXEventEqualityComparer Instance
    {
        get
        {
            if (instance == null)
                instance = new FXEventEqualityComparer();

            return instance;
        }
    }

    private FXEventEqualityComparer() { }

    public bool Equals(FXEvent x, FXEvent y)
    {
        return x.Timestamp == y.Timestamp && x.Title == y.Title && x.Currency == y.Currency;
    }

    public int GetHashCode(FXEvent obj)
    {
        return obj.Timestamp.GetHashCode() + obj.Title.GetHashCode() + obj.Currency.GetHashCode();
    }
}
