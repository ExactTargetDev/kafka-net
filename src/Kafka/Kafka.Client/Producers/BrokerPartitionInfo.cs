namespace Kafka.Client.Producers
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;

    using Kafka.Client.Api;
    using Kafka.Client.Cfg;
    using Kafka.Client.Client;
    using Kafka.Client.Cluster;
    using Kafka.Client.Common;

    using log4net;

    internal class BrokerPartitionInfo
    {

        private ProducerConfig producerConfig;

        private ProducerPool producerPool;

        private Dictionary<string, TopicMetadata> topicPartitionInfo; 

        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private IList<BrokerConfiguration> brokerList;

        private IList<Broker> brokers;

        public BrokerPartitionInfo(ProducerConfig producerConfig, ProducerPool producerPool, Dictionary<string, TopicMetadata> topicPartitionInfo)
        {
            this.producerConfig = producerConfig;
            this.producerPool = producerPool;
            this.topicPartitionInfo = topicPartitionInfo;

            this.brokerList = producerConfig.Brokers;
            this.brokers = ClientUtils.ParseBrokerList(this.brokerList);
        }


        /// <summary>
        /// Return a sequence of (brokerId, numPartitions).
        /// </summary>
        /// <param name="topic"></param>
        /// <returns></returns>
        public IList<PartitionAndLeader> GetBrokerPartitionInfo(string topic, int correlationId)
        {
            Logger.DebugFormat("Getting broker partition info for topic {0}", topic);

            // check if the cache has metadata for this topic
            if (!this.topicPartitionInfo.ContainsKey(topic))
            {
                // refresh the topic metadata cache
                this.UpdateInfo(new HashSet<string> {topic}, correlationId);
                if (!this.topicPartitionInfo.ContainsKey(topic))
                {
                    throw new KafkaException(string.Format("Failed to fetch topic metadata for topic: {0}", topic));
                }
            }
            var metadata = this.topicPartitionInfo[topic];
            var partitionMetadata = metadata.PartitionsMetadata;

            if (partitionMetadata.Count() == 0)
            {
              /* TODO
               * if(metadata.errorCode != ErrorMapping.NoError) {
        throw new KafkaException(ErrorMapping.exceptionFor(metadata.errorCode))
      } else {
        throw new KafkaException("Topic metadata %s has empty partition metadata and no error code".format(metadata))
      }*/
            }

            return partitionMetadata.Select(m =>
            {
                if (m.Leader != null)
                {
                    Logger.DebugFormat("Partition [{0}, {1}] has leader {2}", topic, m.PartitionId, m.Leader.Id);
                    return new PartitionAndLeader(topic, m.PartitionId, m.Leader.Id);
                }
                else
                {
                    Logger.DebugFormat(
                        "Partition [{0}, {1}] does not have a leader yet", topic,
                        m.PartitionId);
                    return new PartitionAndLeader(topic, m.PartitionId, null);
                }
            }
                ).OrderBy(x => x.PartitionId).ToList();
        }

        /// <summary>
        /// It updates the cache by issuing a get topic metadata request to a random broker.
        /// </summary>
        /// <param name="topics"></param>
        /// <param name="correlationId"></param>
        public void UpdateInfo(ISet<string> topics, int correlationId)
        {
            List<TopicMetadata> topicsMetadata = null;
            var topicMetadataResponse = ClientUtils.FetchTopicMetadata(topics, this.brokers, this.producerConfig, correlationId);
            topicsMetadata = topicMetadataResponse.TopicsMetadata;

            foreach (var tmd in topicsMetadata)
            {
                Logger.DebugFormat("Metadata for topic {0} is {1}", tmd.Topic, tmd);
                if (tmd.ErrorCode == ErrorMapping.NoError)
                {
                    this.topicPartitionInfo[tmd.Topic] = tmd;
                }
                else
                {
                    Logger.WarnFormat("Error while fetch metadata [{0}] for topic [{1}]: {2}", tmd, tmd.Topic, ErrorMapping.ExceptionFor(tmd.ErrorCode).GetType());
                    foreach (var pmd in tmd.PartitionsMetadata)
                    {
                        if (pmd.ErrorCode != ErrorMapping.NoError
                            && pmd.ErrorCode == ErrorMapping.LeaderNotAvailableCode)
                        {
                            Logger.WarnFormat("Error while fetching metadata {0} for topic partiton [{1},{2}]:[{3}]", pmd, tmd.Topic, pmd.PartitionId, ErrorMapping.ExceptionFor(pmd.ErrorCode).GetType());
                            //// any other error code (e.g. ReplicaNotAvailable) can be ignored since the producer does not need to access the replica and isr metadata
                        }
                    }
                }
            }

            this.producerPool.UpdateProducer(topicsMetadata);
        }
    }
}