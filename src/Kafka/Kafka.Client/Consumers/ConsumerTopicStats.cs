namespace Kafka.Client.Consumers
{
    using System;
    using System.Diagnostics.CodeAnalysis;

    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Utils;

    public class ConsumerTopicMetrics
    {
        public IMeter MessageRate { get; private set; }

        public IMeter ByteRate { get; private set; }

        public ConsumerTopicMetrics(ClientIdAndTopic metricId)
        {
            this.MessageRate = MetersFactory.NewMeter(metricId + "MessagesPerSec", "messages", TimeSpan.FromSeconds(1));
            this.ByteRate = MetersFactory.NewMeter(metricId + "BytesPerSec", "bytes", TimeSpan.FromSeconds(1));
        }
    }

    /// <summary>
    /// Tracks metrics for each topic the given consumer client has consumed data from.
    /// </summary>
    public class ConsumerTopicStats
    {
        public string ClientId { get; private set; }

        private readonly Func<ClientIdAndTopic, ConsumerTopicMetrics> valueFactory;

        private readonly Pool<ClientIdAndTopic, ConsumerTopicMetrics> stats;

        private readonly ConsumerTopicMetrics allTopicStats;

        /// <summary>
        /// </summary>
        /// <param name="clientId">The clientId of the given consumer client.</param>
        public ConsumerTopicStats(string clientId)
        {
            this.ClientId = clientId;
            this.valueFactory = k => new ConsumerTopicMetrics(k);
            this.stats = new Pool<ClientIdAndTopic, ConsumerTopicMetrics>(this.valueFactory);
            this.allTopicStats = new ConsumerTopicMetrics(new ClientIdAndTopic(clientId, "AllTopics"));
        }

        public ConsumerTopicMetrics GetConsumerAllTopicStats()
        {
            return this.allTopicStats;
        }

        public ConsumerTopicMetrics GetConsumerTopicStats(string topic)
        {
            return this.stats.GetAndMaybePut(new ClientIdAndTopic(this.ClientId, topic + "-"));
        }
    }

    /// <summary>
    /// Stores the topic stats information of each consumer client in a (clientId -> ConsumerTopicStats) map.
    /// </summary>
    public static class ConsumerTopicStatsRegistry
    {
        [SuppressMessage("StyleCop.CSharp.NamingRules", "SA1311:StaticReadonlyFieldsMustBeginWithUpperCaseLetter", Justification = "Reviewed. Suppression is OK here.")]
        private static readonly Func<string, ConsumerTopicStats> valueFactory = k => new ConsumerTopicStats(k);

        [SuppressMessage("StyleCop.CSharp.NamingRules", "SA1311:StaticReadonlyFieldsMustBeginWithUpperCaseLetter", Justification = "Reviewed. Suppression is OK here.")]
        private static readonly Pool<string, ConsumerTopicStats> globalStats = new Pool<string, ConsumerTopicStats>(valueFactory);

        public static ConsumerTopicStats GetConsumerTopicStat(string clientId)
        {
            return globalStats.GetAndMaybePut(clientId);
        }
    }
}