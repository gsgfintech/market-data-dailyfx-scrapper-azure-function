using System;

public static void Run(TimerInfo myTimer, TraceWriter log)
{
    log.Info($"C# Timer trigger function 'keep alive' executed at: {DateTime.Now}");    
}