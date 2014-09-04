namespace Kafka.Client.Cfg
{
    public static class StatSettings
    {
        public static volatile bool ConsumerStatsEnabled = false;

        public static volatile bool ProducerStatsEnabled = false;

        public static volatile bool FetcherThreadStatsEnabled = false;
    }
}
