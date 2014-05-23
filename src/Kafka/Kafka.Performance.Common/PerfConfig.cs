namespace Kafka.Performance.Common
{
    using System;

    using NDesk.Options;

    public abstract class PerfConfig
    {
        public string[] Args { get; protected set; }

        public OptionSet OptionSet { get; set; }

        public long NumMessages { get; set; }

        public int ReportingInterval { get; set; }

        public string DateFormat { get; set; }

        public bool ShowDetailedStats { get; set; }

        public bool HideHeader { get; set; }

        public int MessageSize { get; set; }

        public int BatchSize { get; set; }

        public int CompressionCodecInt { get; set; }

        public PerfConfig()
        {
            this.NumMessages = long.MaxValue;
            this.ReportingInterval = 5000;
            this.DateFormat = "o";
            this.ShowDetailedStats = false;
            this.HideHeader = false;
            this.MessageSize = 100;
            this.BatchSize = 200;
            this.CompressionCodecInt = 0;

            this.OptionSet = new OptionSet
                                 {
                                     { "messages:", "The number of messages to send or consume", x => this.NumMessages = long.Parse(x) },
                                     { "reporting-interval:", "Interval at which to print progress info.", x => this.ReportingInterval = int.Parse(x) },
                                     { "date-format:", "The date format to use for formatting the time field.", x => this.DateFormat = x },
                                     { "show-detailed-stats", "If set, stats are reported for each reporting interval as configured by reporting-interval", _ => this.ShowDetailedStats = true },
                                     { "hide-header", "If set, skips printing the header for the stats", _ => this.HideHeader = true },
                                     { "message-size:", "The size of each message.", x => this.MessageSize = int.Parse(x) },
                                     { "batch-size:", "Number of messages to write in a single batch.", x => this.BatchSize = int.Parse(x) },
                                     { "compression-codec:", "If set, messages are sent compressed. supported codec: NoCompressionCodec as 0, GZIPCompressionCodec as 1, SnappyCompressionCodec as 2", x => this.CompressionCodecInt = int.Parse(x) },
                                     { "?|h|help:", OptionValueCollection =>
                                         {
                                             PrintUsage();
                                             Environment.Exit(0);
                                         }
                                     }
                                 };
        }

        protected void PrintUsage()
        {
            OptionSet.WriteOptionDescriptions(Console.Out);

            Console.WriteLine();
        }

        protected void ParseArguments()
        {
            try
            {
                if (this.Args.Length == 0)
                {
                    this.PrintUsage();
                    Environment.Exit(0);
                }

                this.OptionSet.Parse(this.Args);
            }
            catch (Exception e)
            {
                Console.WriteLine("Could not understand arguments");
                Console.WriteLine(e.Message);
                this.PrintUsage();
                Environment.Exit(1);
            }
        }
    }
}