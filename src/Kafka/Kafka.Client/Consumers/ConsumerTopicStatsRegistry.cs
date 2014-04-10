namespace Kafka.Client.Consumers
{
    using System;

    using Kafka.Client.Utils;

    /// <summary>
    /// Stores the topic stats information of each consumer client in a (clientId -> ConsumerTopicStats) map.
    /// </summary>
    public static class ConsumerTopicStatsRegistry
    {
        private static readonly Func<string, ConsumerTopicStats> ValueFactory = k => new ConsumerTopicStats(k);

        private static readonly Pool<string, ConsumerTopicStats> GlobalStats = new Pool<string, ConsumerTopicStats>(ValueFactory);

        public static ConsumerTopicStats GetConsumerTopicStat(string clientId)
        {
            return GlobalStats.GetAndMaybePut(clientId);
        }
    }
}