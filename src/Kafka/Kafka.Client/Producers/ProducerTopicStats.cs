namespace Kafka.Client.Producers
{
    using System;

    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Utils;

    public class ProducerTopicMetrics
    {
        public IMeter MessageRate { get; private set; }

        public IMeter ByteRate { get; private set; }

        public IMeter DroppedMessageRate { get; private set; }

        public ProducerTopicMetrics(ClientIdAndTopic metricId)
        {
            this.MessageRate = MetersFactory.NewMeter(metricId + "MessagesPerSec", "messages", TimeSpan.FromSeconds(1));
            this.ByteRate = MetersFactory.NewMeter(metricId + "BytesPerSec", "bytes", TimeSpan.FromSeconds(1));
            this.DroppedMessageRate = MetersFactory.NewMeter(
                metricId + "DroppedMessagesPerSec", "drops", TimeSpan.FromSeconds(1));
        }
    }

    public class ProducerTopicStats
    {
        private Func<ClientIdAndTopic, ProducerTopicMetrics> valueFactory;

        private Pool<ClientIdAndTopic, ProducerTopicMetrics> stats;

        private ProducerTopicMetrics allTopicsStats;

        public ProducerTopicStats(string clientId)
        {
            this.ClientId = clientId;
            valueFactory = (k) => new ProducerTopicMetrics(k);
            stats = new Pool<ClientIdAndTopic, ProducerTopicMetrics>(this.valueFactory);
            allTopicsStats = new ProducerTopicMetrics(new ClientIdAndTopic(ClientId, "AllTopics"));
            
        }

        public ProducerTopicMetrics GetProducerAllTopicsStats()
        {
            return allTopicsStats;
        }

        public ProducerTopicMetrics GetProducerTopicStats(string topic)
        {
            return stats.GetAndMaybePut(new ClientIdAndTopic(ClientId, topic + "-"));
        }

        public string ClientId { get; private set; } 

    }

    public static class ProducerTopicStatsRegistry
    {
        private static Func<string, ProducerTopicStats> valueFactory;

        private static Pool<string, ProducerTopicStats> globalStats;

        static ProducerTopicStatsRegistry()
        {
            valueFactory = (k) => new ProducerTopicStats(k);
            globalStats = new Pool<string, ProducerTopicStats>(valueFactory);
        }

        public static ProducerTopicStats GetProducerTopicStats(string clientId)
        {
            return globalStats.GetAndMaybePut(clientId);
        }
    }

}