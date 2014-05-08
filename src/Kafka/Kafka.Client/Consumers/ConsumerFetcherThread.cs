namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Generic;

    using Kafka.Client.Api;
    using Kafka.Client.Clusters;
    using Kafka.Client.Common;
    using Kafka.Client.Extensions;
    using Kafka.Client.Messages;
    using Kafka.Client.Server;

    internal class ConsumerFetcherThread : AbstractFetcherThread
    {
        private readonly ConsumerConfig config;

        private readonly ConsumerFetcherManager consumerFetcherManager;

        private IDictionary<TopicAndPartition, PartitionTopicInfo> partitionMap;

        public ConsumerFetcherThread(
            string name,
            ConsumerConfig config,
            Broker sourceBroker,
            IDictionary<TopicAndPartition, PartitionTopicInfo> partitionMap,
            ConsumerFetcherManager consumerFetcherManager) : base(
            name, 
            config.ClientId + "-" + name, 
            sourceBroker, 
            config.SocketTimeoutMs,
            config.SocketReceiveBufferBytes, 
            config.FetchMessageMaxBytes, 
            Request.OrdinaryConsumerId,
            config.FetchWaitMaxMs,
            config.FetchMinBytes, 
            true)

        {
            this.partitionMap = partitionMap;
            this.config = config;
            this.consumerFetcherManager = consumerFetcherManager;
        }

        public override void ProcessPartitionData(
            TopicAndPartition topicAndPartition, long fetchOffset, FetchResponsePartitionData partitionData)
        {
            var pti = this.partitionMap.Get(topicAndPartition);
            if (pti.GetFetchOffset() != fetchOffset)
            {
                throw new Exception(string.Format("Offset doesn't match for partition [{0},{1}] pti offset: {2} fetch offset: {3}", topicAndPartition.Topic, topicAndPartition.Partiton, pti.GetFetchOffset(), fetchOffset));
            }

            pti.Enqueue((ByteBufferMessageSet)partitionData.Messages);
        }

        public override long HandleOffsetOutOfRange(TopicAndPartition topicAndPartition)
        {
            long startTimestamp;
            switch (this.config.AutoOffsetReset)
            {
                case OffsetRequest.SmallestTimeString:
                    startTimestamp = OffsetRequest.EarliestTime;
                    break;
                case OffsetRequest.LargestTimeString:
                    startTimestamp = OffsetRequest.LatestTime;
                    break;
                default:
                    startTimestamp = OffsetRequest.LatestTime;
                    break;
            }

            var newOffset = simpleConsumer.EarliestOrLatestOffset(
                topicAndPartition, startTimestamp, Request.OrdinaryConsumerId);
            var pti = this.partitionMap.Get(topicAndPartition);
            pti.ResetFetchOffset(newOffset);
            pti.ResetConsumeOffset(newOffset);
            return newOffset;
        }

        public override void HandlePartitionsWithErrors(IEnumerable<TopicAndPartition> partitions)
        {
            this.RemovePartitions(new HashSet<TopicAndPartition>(partitions));
            this.consumerFetcherManager.AddPartitionsWithError(partitions);
        }
    }
}