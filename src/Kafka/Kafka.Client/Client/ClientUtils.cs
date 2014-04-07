namespace Kafka.Client.Client
{
    using System;
    using System.Collections.Generic;

    using Kafka.Client.Api;
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;

    using System.Linq;

    using Kafka.Client.Utils;

    /// <summary>
    ///  Helper functions common to clients (producer, consumer, or admin)
    /// </summary>
    public static class ClientUtils
    {

        public static TopicMetadataResponse FetchTopicMetadata(
            ISet<string> topics, IList<Broker> brokers, ProducerConfiguration producerConfig, int correlationId)
        {
            throw new NotImplementedException();
        }

        public static TopicMetadataResponse FetchTopicMetadata(
            ISet<string> topics, List<Broker> brokers, string clientId, int timeoutMs, int correlationId = 0)
        {

            throw new NotImplementedException();
        }



        /// <summary>
        /// Parse a list of broker urls in the form host1:port1, host2:port2, ... 
        /// </summary>
        /// <param name="brokerListStr"></param>
        /// <returns></returns>
        public static IList<Broker> ParseBrokerList(IList<BrokerConfiguration> brokerListStr)
        {
            return brokerListStr.Select(conf => new Broker(conf.BrokerId, conf.Host, conf.Port)).ToList();
        }  
    }
}