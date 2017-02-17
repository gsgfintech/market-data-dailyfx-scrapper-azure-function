using Capital.GSG.FX.Data.Core.MarketData;
using System;

public static void Run(TimerInfo myTimer, TraceWriter log)
{
    log.Info($"C# Timer trigger function executed at: {DateTime.Now}");

    FXEvent fxEvent = new FXEvent()
    {
        EventId = "test"
    };

    log.Info($"Created FX event with id {fxEvent.EventId}");
}