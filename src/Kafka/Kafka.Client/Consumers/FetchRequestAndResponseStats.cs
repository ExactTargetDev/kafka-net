namespace Kafka.Client.Consumers
{
    using System;

    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Metrics;
    using Kafka.Client.Utils;

    internal class FetchRequestAndResponseMetrics
    {
        private ClientIdAndBroker metricId;

        public KafkaTimer RequestTimer { get; private set; }

        public IHistogram RequestSizeHist { get; private set; }

        public FetchRequestAndResponseMetrics(ClientIdAndBroker metricId)
        {
            this.metricId = metricId;
            this.RequestTimer = new KafkaTimer(MetersFactory.NewTimer(metricId + "FetchRequestRateAndTimeMs", TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1)));
            this.RequestSizeHist = MetersFactory.NewHistogram(metricId + "FetchResponseSize");
        }
    }

    /// <summary>
    /// Tracks metrics of the requests made by a given consumer client to all brokers, and the responses obtained from the brokers.
    /// </summary>
    internal class FetchRequestAndResponseStats
    {
        private string clientId;

        private Func<ClientIdAndBroker, FetchRequestAndResponseMetrics> valueFactory;

        private Pool<ClientIdAndBroker, FetchRequestAndResponseMetrics> stats;

        private FetchRequestAndResponseMetrics allBrokerStats;

        public FetchRequestAndResponseStats(string clientId)
        {
            this.clientId = clientId;
            this.valueFactory = k => new FetchRequestAndResponseMetrics(k);
            this.stats = new Pool<ClientIdAndBroker, FetchRequestAndResponseMetrics>(valueFactory);
            this.allBrokerStats = new FetchRequestAndResponseMetrics(new ClientIdAndBroker(clientId, "AllBrokers"));
        }

        public FetchRequestAndResponseMetrics GetFetchRequestAndResponseAllBrokersStats()
        {
            return allBrokerStats;
        }

        public FetchRequestAndResponseMetrics GetFetchRequestAndResponseStats(string brokerInfo)
        {
            return stats.GetAndMaybePut(new ClientIdAndBroker(clientId, brokerInfo + "-"));
        }
    }

    internal static class FetchRequestAndResponseStatsRegistry
    {
        private static Func<string, FetchRequestAndResponseStats> valueFactory;

        private static Pool<string, FetchRequestAndResponseStats> globalStas;

        static FetchRequestAndResponseStatsRegistry()
        {
            valueFactory = k => new FetchRequestAndResponseStats(k);
            globalStas = new Pool<string, FetchRequestAndResponseStats>(valueFactory);
        }

        public static FetchRequestAndResponseStats GetFetchRequestAndResponseStats(string clientId)
        {
            return globalStas.GetAndMaybePut(clientId);
        }
    }
}