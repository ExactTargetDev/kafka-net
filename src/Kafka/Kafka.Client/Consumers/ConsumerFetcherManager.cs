namespace Kafka.Client.Consumers
{
    using System;

    using Kafka.Client.Cfg;
    using Kafka.Client.Clusters;
    using Kafka.Client.Server;
    using Kafka.Client.ZKClient;

    public class ConsumerFetcherManager : AbstractFetcherManager
    {
        private readonly string consumerIdString;

        private readonly ConsumerConfiguration config;

        private readonly ZkClient zkClient;

        public ConsumerFetcherManager(string consumerIdString, ConsumerConfiguration config, ZkClient zkClient)
            : base(
                string.Format("ConsumerFetcherManager-{0}", DateTime.Now), config.ClientId, config.NumConsumerFetchers)

    {
        this.consumerIdString = consumerIdString;
        this.config = config;
        this.zkClient = zkClient;
    }

        //TODO:
        public override AbstractFetcherThread CreateFetcherThread(int fetcherId, Broker sourceBroker)
        {
            throw new NotImplementedException();
        }
    }


}