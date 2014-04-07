namespace Kafka.Client.Client
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    using Kafka.Client.Api;
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;

    using System.Linq;

    using Kafka.Client.Common;
    using Kafka.Client.Producers;
    using Kafka.Client.Utils;

    using Kafka.Client.Extensions;

    using log4net;

    /// <summary>
    ///  Helper functions common to clients (producer, consumer, or admin)
    /// </summary>
    public static class ClientUtils
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        /// <summary>
        /// Used by the producer to send a metadata request since it has access to the ProducerConfig
        /// </summary>
        /// <param name="topics"></param>
        /// <param name="brokers"></param>
        /// <param name="producerConfig"></param>
        /// <param name="correlationId"></param>
        /// <returns></returns>
        public static TopicMetadataResponse FetchTopicMetadata(
            ISet<string> topics, IList<Broker> brokers, ProducerConfig producerConfig, int correlationId)
        {
            var fetchMetaDataSucceeded = false;
            var i = 0;
            var topicMetadataRequest = new TopicMetadataRequest(
                TopicMetadataRequest.CurrentVersion, correlationId, producerConfig.ClientId, topics.ToList());

            TopicMetadataResponse topicMetadataResponse = null;
            Exception t = null;
            // shuffle the list of brokers before sending metadata requests so that most requests don't get routed to the
            // same broker

            var shuffledBrokers = brokers.Shuffle();
            while (i < shuffledBrokers.Count() && !fetchMetaDataSucceeded)
            {
                var producer = ProducerPool.CreateSyncProducer(producerConfig, shuffledBrokers[i]);
                Logger.InfoFormat("Fetching metadata from broker {0} with correlation id {1} for {2} topic(s) {3}", shuffledBrokers[i], correlationId, topics.Count, topics);
                try
                {
                    topicMetadataResponse = producer.Send(topicMetadataRequest);
                    fetchMetaDataSucceeded = true;
                }
                catch (Exception e)
                {
                    Logger.WarnFormat(
                        "Fetching topic metadata with correlation id {0} for topic [{1}] from broker [{2}] failed",
                        correlationId,
                        topics,
                        shuffledBrokers[i],
                        e);
                    t = e;
                }
                finally
                {
                    i++;
                    producer.Dispose();
                }
            }


            if (!fetchMetaDataSucceeded)
            {
                throw new KafkaException(
                    string.Format(
                        "fetching topic metadata for topics [{0}] from broker [{1}] failed", topics, shuffledBrokers),
                    t);
            }
            else
            {
                Logger.DebugFormat("Successfully fetched metadata for {0} topic(s) {1}", topics.Count(), topics);
            }
            return topicMetadataResponse;
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