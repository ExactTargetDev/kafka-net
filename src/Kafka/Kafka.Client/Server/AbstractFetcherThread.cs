namespace Kafka.Client.Server
{
    using System;
    using System.Collections.Generic;

    using Kafka.Client.Clusters;
    using Kafka.Client.Common;
    using Kafka.Client.Utils;

    public class AbstractFetcherThread : ShutdownableThread
    {
         //TODO:

        public void Start()
        {
            throw new NotImplementedException();
        }

        public AbstractFetcherThread(string name,
            string clientId,
            Broker sourceBroker,
            int socketTimeout,
            int socketBufferSize,
            int fetchSize,
            int fetcherBrokerId = -1,
            int maxWait = 0,
            int minBytes = 1,
            bool isInterruptible = true)
            : base(name, isInterruptible)
        {
            //TODO:
        }

        public override void DoWork()
        {
            throw new NotImplementedException();
        }

        public void AddPartitions(IDictionary<TopicAndPartition, long> partitionAndOffsets)
        {
            throw new NotImplementedException();
        }

        public void RemotePartitions(ISet<TopicAndPartition> topicAndPartitions)
        {
            throw new NotImplementedException();
        }

        public int PartitionCount()
        {
            throw new NotImplementedException();
        }



    }
}