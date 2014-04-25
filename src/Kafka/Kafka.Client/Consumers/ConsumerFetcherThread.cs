namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Generic;

    using Kafka.Client.Api;
    using Kafka.Client.Cfg;
    using Kafka.Client.Clusters;
    using Kafka.Client.Common;
    using Kafka.Client.Server;

    public class ConsumerFetcherThread : AbstractFetcherThread
    {
        private ConsumerConfiguration config;

        private ConsumerFetcherManager consumerFetcherManager;

        public ConsumerFetcherThread(
            string name,
            ConsumerConfiguration config,
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
            config.FetchMinBytes, true)

        {
            throw new NotImplementedException();
        }
    }
}