using Capital.GSG.FX.Utils.Core.Logging;
using log4net.Config;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DailyFXScrapperConsoleApp
{
    class Program
    {
        static void Main(string[] args)
        {
            XmlConfigurator.Configure();

            GSGLoggerFactory.Instance.AddConsole();

            ScrapFXEvents.Function.Run();
        }
    }
}
