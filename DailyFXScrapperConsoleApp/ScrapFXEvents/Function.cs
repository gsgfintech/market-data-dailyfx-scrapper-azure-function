using AngleSharp.Parser.Html;
using Capital.GSG.FX.Data.Core.ContractData;
using Capital.GSG.FX.Data.Core.MarketData;
using Capital.GSG.FX.InfluxDBConnector;
using Capital.GSG.FX.Monitoring.Server.Connector;
using Capital.GSG.FX.Utils.Core;
using log4net;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DailyFXScrapperConsoleApp.ScrapFXEvents
{
    class Function
    {
        private const string DailyFX = "https://www.dailyfx.com";
        private const string CalendarEndpoint = "/calendar";

        private static ILog logger = LogManager.GetLogger(nameof(Function));

        public static void Run()
        {
            logger.Info("Starting DailyFX Scrapper");

            List<FXEvent> events = ScrapFXEvents();

            if (!events.IsNullOrEmpty())
            {
                try
                {
                    PostEventsToMonitoringBackendServer(events).Wait();

                    InsertEventsInInfluxDb(events).Wait();
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

                string clientId = GetEnvironmentVariable("Monitoring:ClientId");
                string appKey = GetEnvironmentVariable("Monitoring:AppKey");

                string backendAddress = GetEnvironmentVariable("Monitoring:BackendAddress");
                string backendAppUri = GetEnvironmentVariable("Monitoring:BackendAppIdUri");

                BackendFXEventsConnector connector = (new MonitoringServerConnector(clientId, appKey, backendAddress, backendAppUri)).FXEventsConnector;

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

        private static async Task InsertEventsInInfluxDb(IEnumerable<FXEvent> events)
        {
            try
            {
                InfluxDBServer dbServer = SetupInfluxDbServer();

                if (dbServer != null)
                {
                    int successCount = 0;
                    int failedCount = 0;

                    logger.Info($"About to add/update {events.Count()} FX Events");

                    CancellationTokenSource cts;

                    foreach (var fxEvent in events)
                    {
                        try
                        {
                            cts = new CancellationTokenSource();
                            cts.CancelAfter(TimeSpan.FromSeconds(10));

                            var result = await dbServer.FXEventsActioner.Insert(fxEvent, cts.Token);

                            if (result.Success)
                                logger.Info($"Successfully added/updated {fxEvent} in the database (success: {++successCount}, failed: {failedCount})");
                            else
                                logger.Error($"Failed to add/update {fxEvent} in the database (success: {successCount}, failed: {++failedCount}): {result.Message}");
                        }
                        catch (Exception ex)
                        {
                            logger.Error($"Failed to add/update {fxEvent} in the database (success: {successCount}, failed: {++failedCount}): {ex.Message}");
                        }
                    }

                    if (successCount > 0 && failedCount == 0)
                        logger.Info($"Successfully added/updated {events.Count()} FX Events");
                    else
                        logger.Error($"Failed to add/update {events.Count()} events in the database (success: {successCount}, failed: {failedCount})");
                }
                else
                    logger.Error("Found no event to process. Will exit");
            }
            catch (Exception ex)
            {
                logger.Error($"Failed to insert events in InfluxDB: {ex.Message}");
            }
        }

        private static InfluxDBServer SetupInfluxDbServer()
        {
            try
            {
                string host = GetEnvironmentVariable("InfluxDB:Host");
                string dbName = GetEnvironmentVariable("InfluxDB:Name");
                string user = GetEnvironmentVariable("InfluxDB:User");
                string password = GetEnvironmentVariable("InfluxDB:Password");

                logger.Info($"Setup InfluxDB on {host}/{dbName} with user {user}");

                return new InfluxDBServer(host, user, password, dbName);
            }
            catch (Exception ex)
            {
                logger.Error($"Failed to setup InfluxDB server: {ex.Message}");
                return null;
            }
        }

        private static string FormatFXEvent(FXEvent fxEvent)
        {
            return $"{fxEvent.Timestamp:dd/MM/yy HH:mm:ss zzz} - {fxEvent.Currency} - {fxEvent.Title}";
        }

        private static string GetEnvironmentVariable(string name)
        {
            // Override this in run.csx file
            return ConfigurationManager.AppSettings[name];
            //return Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
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
    }
}
