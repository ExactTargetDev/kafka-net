namespace Kafka.Client.Producers
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    using Kafka.Client.Api;
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Common;

    using log4net;

    internal class ProducerPool : IDisposable
    {

        private ProducerConfig config;

        private Dictionary<int, SyncProducer> syncProducers;

        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private object @lock = new object();


        public ProducerPool(ProducerConfig config)
        {
            this.config = config;
            this.syncProducers = new Dictionary<int, SyncProducer>();
        }

        public static SyncProducer CreateSyncProducer(ProducerConfig config, Broker broker)
        {
            return new SyncProducer(new SyncProducerConfiguration(config, broker.Host, broker.Port));
        }

        public void UpdateProducer(List<TopicMetadata> topicMetadata)
        {
            //TODO: finish me
        }

        public SyncProducer GetProducer(int brokerId)
        {
            lock (@lock)
            {
                SyncProducer producer = null;
                if (this.syncProducers.TryGetValue(brokerId, out producer))
                {
                    return producer;
                }
                else
                {
                    throw new UnavailableProducerException(string.Format("Sync producer for broker id {0} does not exist", brokerId));
                }
            }
        }

        public void Dispose()
        {
            lock (@lock)
            {
                Logger.DebugFormat("Closing app sync producers");
                foreach (var producer in this.syncProducers.Values)
                {
                    producer.Dispose();
                }
            }
        }

    }
}