namespace Kafka.Client.Producers
{
    using System;

    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Metrics;
    using Kafka.Client.Utils;

    internal class ProducerRequestMetrics
    {
        public KafkaTimer RequestTimer { get; private set; }

        public IHistogram RequestSizeHist { get; private set; }

        public ProducerRequestMetrics(ClientIdAndBroker metricId)
        {
            this.RequestTimer = new KafkaTimer(MetersFactory.NewTimer(metricId + "ProducerRequestRateAndTimeMs", TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1)));
            this.RequestSizeHist = MetersFactory.NewHistogram(metricId + "ProducerRequestSize");
        }
    }

    /// <summary>
    /// Tracks metrics of requests made by a given producer client to all brokers.
    /// </summary>
    internal class ProducerRequestStats
    {
        private readonly Func<ClientIdAndBroker, ProducerRequestMetrics> valueFactory;

        private readonly Pool<ClientIdAndBroker, ProducerRequestMetrics> stats;

        private readonly ProducerRequestMetrics allBrokersStats;

        private readonly string clientId;

        public ProducerRequestStats(string clientId)
        {
            this.clientId = clientId;
        }

        public ProducerRequestStats()
        {
            this.valueFactory = k => new ProducerRequestMetrics(k);
            this.stats = new Pool<ClientIdAndBroker, ProducerRequestMetrics>(this.valueFactory);
            this.allBrokersStats = new ProducerRequestMetrics(new ClientIdAndBroker(this.clientId, "AllBrokers"));
        }

        public ProducerRequestMetrics GetProducerRequestAllBrokersStats()
        {
            return this.allBrokersStats;
        }

        public ProducerRequestMetrics GetProducerRequestStats(string brokerInfo)
        {
            return this.stats.GetAndMaybePut(new ClientIdAndBroker(this.clientId, brokerInfo + "-"));
        }
    }

    /// <summary>
    /// Stores the request stats information of each producer client in a (clientId -> ProducerRequestStats) map.
    /// </summary>
    internal static class ProducerRequestStatsRegistry
    {
        private static Func<string, ProducerRequestStats> valueFactory;

        private static Pool<string, ProducerRequestStats> globalStats;

        static ProducerRequestStatsRegistry()
        {
            valueFactory = k => new ProducerRequestStats(k);
            globalStats = new Pool<string, ProducerRequestStats>(valueFactory);
        }

        public static ProducerRequestStats GetProducerRequestStats(string clientId)
        {
            return globalStats.GetAndMaybePut(clientId);
        }
    }
}