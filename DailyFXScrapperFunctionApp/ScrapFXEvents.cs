using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using System.Collections.Generic;
using Capital.GSG.FX.Data.Core.MarketData;
using Capital.GSG.FX.Utils.Core;
using RestSharp;
using System.Net;
using AngleSharp.Parser.Html;
using Capital.GSG.FX.Data.Core.ContractData;
using System.Threading.Tasks;
using System.Linq;
using Capital.GSG.FX.Monitoring.Server.Connector;

namespace DailyFXScrapperFunctionApp
{
    public static class ScrapFXEvents
    {
        private const string DailyFX = "https://www.dailyfx.com";
        private const string CalendarEndpoint = "/calendar";

        [FunctionName("ScrapFXEvents")]
        public static void Run([TimerTrigger("30 1 * * * *")]TimerInfo myTimer, TraceWriter log)
        {
            log.Info("Starting DailyFX Scrapper");

            List<FXEvent> events = ScrapDailyFXEvents(log);

            if (!events.IsNullOrEmpty())
            {
                try
                {
                    string clientId = GetEnvironmentVariable("Monitoring:ClientId");
                    string appKey = GetEnvironmentVariable("Monitoring:AppKey");

                    #region DevBackend
                    log.Info($"About to add/update events in DEV database");

                    string devBackendAddress = GetEnvironmentVariable("Monitoring:Dev:BackendAddress");
                    string devBackendAppUri = GetEnvironmentVariable("Monitoring:Dev:BackendAppIdUri");

                    PostEventsToMonitoringBackendServer(events, clientId, appKey, devBackendAddress, devBackendAppUri, log).Wait();
                    #endregion

                    #region QABackend
                    log.Info($"About to add/update events in QA database");

                    string qaBackendAddress = GetEnvironmentVariable("Monitoring:QA:BackendAddress");
                    string qaBackendAppUri = GetEnvironmentVariable("Monitoring:QA:BackendAppIdUri");

                    PostEventsToMonitoringBackendServer(events, clientId, appKey, qaBackendAddress, qaBackendAppUri, log).Wait();
                    #endregion

                    #region ProdBackend
                    log.Info($"About to add/update events in PROD database");

                    string prodBackendAddress = GetEnvironmentVariable("Monitoring:Prod:BackendAddress");
                    string prodBackendAppUri = GetEnvironmentVariable("Monitoring:Prod:BackendAppIdUri");

                    PostEventsToMonitoringBackendServer(events, clientId, appKey, prodBackendAddress, prodBackendAppUri, log).Wait();
                    #endregion

                    InsertEventsInInfluxDb(events, clientId, appKey, prodBackendAddress, prodBackendAppUri, log).Wait();
                }
                catch (Exception ex)
                {
                    log.Error($"Failed to add events to SQL Server: {ex.Message}");
                }
            }
            else
                log.Error("Found no event to process. Will exit");
        }

        private static List<FXEvent> ScrapDailyFXEvents(TraceWriter log)
        {
            string source = LoadPage(log);

            if (!string.IsNullOrEmpty(source))
                return ParseEvents(source, log);
            else
                log.Error("Failed to read the page");

            return null;
        }

        private static string LoadPage(TraceWriter log)
        {
            try
            {
                log.Info($"Loading DailyFX calendar from {DailyFX}{CalendarEndpoint}");

                RestClient client = new RestClient(DailyFX);

                var request = new RestRequest(CalendarEndpoint, Method.GET);

                IRestResponse response = client.Execute(request);

                if (response != null && response.StatusCode == HttpStatusCode.OK && !string.IsNullOrEmpty(response.Content))
                {
                    log.Info("Successfully loaded the page");
                    return response.Content;
                }
                else
                {
                    log.Error($"Failed to load calendar page (response {response?.StatusCode})");
                    return null;
                }
            }
            catch (Exception ex)
            {
                log.Error("Caught exception in LoadPage()", ex);
                return null;
            }
        }

        private static List<FXEvent> ParseEvents(string source, TraceWriter log)
        {
            try
            {
                log.Info("Parsing events from the source");

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
                        log.Error("Failed to parse event", ex);
                    }
                }

                log.Info($"Parsed {events.Count} events");

                return events;
            }
            catch (Exception ex)
            {
                log.Error("Caught exception in ParseEvents()", ex);
                return null;
            }
        }

        private static async Task PostEventsToMonitoringBackendServer(List<FXEvent> events, string clientId, string appKey, string backendAddress, string backendAppUri, TraceWriter log)
        {
            try
            {
                events.Sort(CompareFXEventsTimestamps);

                DateTimeOffset min = events.Select(e => e.Timestamp).Min();
                DateTimeOffset max = events.Select(e => e.Timestamp).Max();

                BackendFXEventsConnector connector = (new MonitoringServerConnector(clientId, appKey, backendAddress, backendAppUri)).FXEventsConnector;

                List<FXEvent> existing = await connector.GetInTimeRange(min, max);

                if (existing.IsNullOrEmpty())
                {
                    log.Info("Found no existing events in DB. Will add the whole batch as new events");

                    foreach (var fxEvent in events)
                    {
                        var result = await connector.AddOrUpdate(fxEvent);

                        if (!result.Success)
                            log.Error($"Failed to add event '{fxEvent.Title}': {result.Message}");
                    }

                    log.Info($"Successfully processed {events.Count} events");
                }
                else
                {
                    log.Info($"Found {existing.Count} existing events in DB between {min} and {max}");

                    List<FXEvent> toProcess = new List<FXEvent>();

                    var toAdd = events.Except(existing, FXEventEqualityComparer.Instance);

                    if (!toAdd.IsNullOrEmpty())
                    {
                        log.Info($"Found {toAdd.Count()} new events that are not in DB. Will add them");

                        foreach (var fxEvent in toAdd)
                        {
                            var result = await connector.AddOrUpdate(fxEvent);

                            if (!result.Success)
                                log.Error($"Failed to add event '{fxEvent.Title}': {result.Message}");
                        }

                        toProcess.AddRange(toAdd);
                    }

                    var toCheck = events.Intersect(existing, FXEventEqualityComparer.Instance);

                    if (!toCheck.IsNullOrEmpty())
                    {
                        log.Info($"Found {toCheck.Count()} events already in DB. Will check them one by one");

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
                                log.Info($"Event {FormatFXEvent(fxEvent)} was updated on DailyFX and needs to be updated in the DB");
                                log.Info($"\tPrev level: {current.Level}, New level: {updated.Level}");
                                log.Info($"\tPrev explanation: {current.Explanation}, New explanation: {updated.Explanation}");
                                log.Info($"\tPrev forecast: {current.Forecast}, New forecast: {updated.Forecast}");
                                log.Info($"\tPrev previous: {current.Previous}, New previous: {updated.Previous}");
                                log.Info($"\tPrev actual: {current.Actual}, New actual: {updated.Actual}");

                                toProcess.Add(fxEvent);
                            }
                        }
                    }

                    if (!toProcess.IsNullOrEmpty())
                    {
                        foreach (var fxEvent in toProcess)
                        {
                            var result = await connector.AddOrUpdate(fxEvent);

                            if (!result.Success)
                                log.Error($"Failed to update event '{fxEvent.Title}': {result.Message}");
                        }

                        log.Info($"Successfully processed {toProcess.Count} events");
                    }
                    else
                        log.Info("All events received from DailyFX, but none was found to be new or updated. Nothing to add/update in DB");
                }
            }
            catch (Exception ex)
            {
                log.Error($"Failed to process events: {ex}");
            }
        }

        private static async Task InsertEventsInInfluxDb(IEnumerable<FXEvent> events, string clientId, string appKey, string backendAddress, string backendAppUri, TraceWriter log)
        {
            try
            {
                BackendFXEventsConnector connector = (new MonitoringServerConnector(clientId, appKey, backendAddress, backendAppUri)).FXEventsConnector;

                string host = GetEnvironmentVariable("InfluxDB:Host");
                string dbName = GetEnvironmentVariable("InfluxDB:Name");
                string user = GetEnvironmentVariable("InfluxDB:User");

                var result = await connector.InsertInOnSiteInfluxDB(events, host, dbName, user);

                if (result.Success)
                    log.Info($"Successfully added/updated {events.Count()} in onsite InfluxDB");
                else
                    log.Error($"Failed to add/update {events.Count()} in onsite InfluxDB: {result.Message}");
            }
            catch (Exception ex)
            {
                log.Error($"Failed to insert events in InfluxDB: {ex.Message}");
            }
        }

        private static string FormatFXEvent(FXEvent fxEvent)
        {
            return $"{fxEvent.Timestamp:dd/MM/yy HH:mm:ss zzz} - {fxEvent.Currency} - {fxEvent.Title}";
        }

        private static string GetEnvironmentVariable(string name)
        {
            return Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
        }

        private static int CompareFXEventsTimestamps(FXEvent x, FXEvent y)
        {
            return x.Timestamp.CompareTo(y.Timestamp);
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