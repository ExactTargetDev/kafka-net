namespace Kafka.Client.Producers
{
    using System;

    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Utils;

    public class ProducerStats
    {
        public string ClientId { get; private set; }

        public IMeter SerializationErrorRate { get; private set; }

        public IMeter ResendRate { get; private set; }

        public IMeter FailedSendRate { get; private set; }

        public ProducerStats(string clientId)
        {
            this.ClientId = clientId;
            this.SerializationErrorRate = MetersFactory.NewMeter(
                clientId + "-SerializationErrorsPerSec", "errors", TimeSpan.FromSeconds(1));

            this.ResendRate = MetersFactory.NewMeter(clientId + "-ResendsPerSec", "resends", TimeSpan.FromSeconds(1));

            this.FailedSendRate = MetersFactory.NewMeter(
                clientId + "-FailedSendsPerSec", "failed sends", TimeSpan.FromSeconds(1));
        }
    }

    /// <summary>
    /// Stores metrics of serialization and message sending activity of each producer client in a (clientId -> ProducerStats) map.
    /// </summary>
    public static class ProducerStatsRegistry
    {
        private static Func<string, ProducerStats> valueFactory;

        private static Pool<string, ProducerStats> statsRegistry; 

        static ProducerStatsRegistry() 
        {
            valueFactory = k => new ProducerStats(k);
            statsRegistry = new Pool<string, ProducerStats>(valueFactory);
        }

        public static ProducerStats GetProducerStats(string clientId)
        {
            return statsRegistry.GetAndMaybePut(clientId);
        }
    }
}