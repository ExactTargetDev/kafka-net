using System;
using Kafka.Client.Utils;

namespace Kafka.Client.Consumers
{
    /// <summary>
    /// Stores the topic stats information of each consumer client in a (clientId -> ConsumerTopicStats) map.
    /// </summary>
    public static class ConsumerTopicStatsRegistry
    {
        private static readonly Func<string, ConsumerTopicStats> valueFactory = (k) => new ConsumerTopicStats(k);
        private static readonly Pool<String, ConsumerTopicStats> globalStats = new Pool<string, ConsumerTopicStats>(valueFactory);

        public static ConsumerTopicStats GetConsumerTopicStat(string clientId)
        {
            return globalStats.GetAndMaybePut(clientId);
        }

    }
}