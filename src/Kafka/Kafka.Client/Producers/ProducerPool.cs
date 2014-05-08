namespace Kafka.Client.Producers
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    using Kafka.Client.Api;
    using Kafka.Client.Clusters;
    using Kafka.Client.Common;

    using log4net;

    internal class ProducerPool : IDisposable
    {
        public static SyncProducer CreateSyncProducer(ProducerConfig config, Broker broker)
        {
            return new SyncProducer(new SyncProducerConfig(config, broker.Host, broker.Port));
        }

        private readonly ProducerConfig config;

        private readonly Dictionary<int, SyncProducer> syncProducers;

        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly object @lock = new object();

        public ProducerPool(ProducerConfig config)
        {
            this.config = config;
            this.syncProducers = new Dictionary<int, SyncProducer>();
        }

        public void UpdateProducer(List<TopicMetadata> topicMetadata)
        {
            var newBrokers = new HashSet<Broker>();
            foreach (var tmd in topicMetadata)
            {
                foreach (var pmd in tmd.PartitionsMetadata)
                {
                    if (pmd.Leader != null)
                    {
                        newBrokers.Add(pmd.Leader);
                    }
                }
            }

            lock (@lock)
            {
                foreach (var b in newBrokers)
                {
                    if (this.syncProducers.ContainsKey(b.Id))
                    {
                        this.syncProducers[b.Id].Dispose();
                        this.syncProducers[b.Id] = CreateSyncProducer(this.config, b);
                    }
                    else
                    {
                        this.syncProducers[b.Id] = CreateSyncProducer(this.config, b);
                    }
                }
            }
        }

        public SyncProducer GetProducer(int brokerId)
        {
            lock (@lock)
            {
                SyncProducer producer;
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