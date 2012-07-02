// -----------------------------------------------------------------------
// <copyright file="BrokerPartitionInfo.cs" company="">
// TODO: Update copyright text.
// </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Kafka.Client.Cluster;
using Kafka.Client.Exceptions;
using Kafka.Client.Requests;
using Kafka.Client.ZooKeeperIntegration;
using log4net;
using Kafka.Client.Utils;

namespace Kafka.Client.Producers.Partitioning
{
    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class BrokerPartitionInfo
    {
        private ProducerPool producerPool;
        private IDictionary<string, TopicMetadata> topicPartitionInfo = new Dictionary<string, TopicMetadata>();
        private ZooKeeperClient zkClient;
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        internal BrokerPartitionInfo(ProducerPool producerPool)
        {
            this.producerPool = producerPool;
            this.zkClient = this.producerPool.GetZkClient();
        }

        public IEnumerable<Partition> GetBrokerPartitionInfo(string topic)
        {
            Logger.DebugFormat("Getting broker partition info for topic {0}", topic);
            //check if the cache has metadata for this topic
            if (!this.topicPartitionInfo.ContainsKey(topic))
            {
                //refresh the topic metadata cache
                Logger.InfoFormat("Fetching metadata for topic {0}", topic);
                this.UpdateInfo(topic);
                if (!this.topicPartitionInfo.ContainsKey(topic))
                {
                    throw new IllegalStateException(string.Format("Failed to fetch topic metadata for topic: {0}", topic));
                }
            }
            var metadata = this.topicPartitionInfo[topic];
            var partitionMetadata = metadata.PartitionsMetadata;
            return partitionMetadata.Select(m =>
                                         {
                                             var partition = new Partition(topic, m.PartitionId);
                                             if (m.Leader != null)
                                             {
                                                 var leaderReplica = new Replica(m.Leader.Id, partition, topic);
                                                 partition.Leader = leaderReplica;
                                                 Logger.DebugFormat("Topic {0} partition {1} has leader {2}", topic,
                                                                    m.PartitionId, m.Leader.Id);
                                                 return partition;
                                             }
                                             else
                                             {
                                                 Logger.DebugFormat(
                                                     "Topic {0} partition {1} does not have a leader yet", topic,
                                                     m.PartitionId);
                                                 return partition;
                                             }
                                         }
                ).OrderBy(x => x.PartId);
        }

        public void UpdateInfo(string topic = null)
        {
            var producer = this.producerPool.GetAnyProducer();
            if (topic != null)
            {
                var topicMetadataRequest = new TopicMetadataRequest(new List<string>() { topic });
                var topicMetadataList = producer.Send(topicMetadataRequest);
                var topicMetadata = topicMetadataList.Count() > 0 ? topicMetadataList.First() : null;
                if (topicMetadata != null)
                {
                    Logger.InfoFormat("Fetched metadata for topics {0}", topic);
                    this.topicPartitionInfo[topic] = topicMetadata;
                }
            }
            else
            {
                //refresh cache for all topics
                var topics = topicPartitionInfo.Select(x => x.Key);
                var topicMetadata = producer.Send(new TopicMetadataRequest(topics));
                topicMetadata.ForEach(metadata => this.topicPartitionInfo[metadata.Topic] = metadata);
            }
        }
    }
}
