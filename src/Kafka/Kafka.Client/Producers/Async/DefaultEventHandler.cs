namespace Kafka.Client.Producers.Async
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Threading;

    using Kafka.Client.Api;
    using Kafka.Client.Cfg;
    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Messages;
    using Kafka.Client.Serializers;
    using Kafka.Client.Utils;

    using log4net;

    using System.Linq;

    internal class DefaultEventHandler<TKey, TValue> : IEventHandler<TKey, TValue>
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private Random random = new Random();

        public ProducerConfig config { get; set; }

        private IPartitioner partitioner;

        private IEncoder<TValue> encoder;

        private IEncoder<TKey> keyEncoder;

        private ProducerPool producerPool;

        private Dictionary<string, TopicMetadata> topicPartitionInfos = new Dictionary<string, TopicMetadata>();

        public DefaultEventHandler(ProducerConfig config, IPartitioner partitioner, IEncoder<TValue> encoder, IEncoder<TKey> keyEncoder, ProducerPool producerPool)
        {
            this.config = config;
            this.partitioner = partitioner;
            this.encoder = encoder;
            this.keyEncoder = keyEncoder;
            this.producerPool = producerPool;

            this.brokerPartitionInfo = new BrokerPartitionInfo(this.config, this.producerPool, this.topicPartitionInfos);
            this.topicMetadataRefreshInterval = TimeSpan.FromMilliseconds(config.TopicMetadataRefreshIntervalMs);

            this.producerStats = ProducerStatsRegistry.GetProducerStats(config.ClientId);
            this.producerTopicStats = ProducerTopicStatsRegistry.GetProducerTopicStats(config.ClientId);

        }

        public bool IsSync 
        {
            get
            {
                return this.config.ProducerType == ProducerTypes.Sync;
            }
        }

        private AtomicInteger correlationId = new AtomicInteger(0);

        private readonly BrokerPartitionInfo brokerPartitionInfo;

        private readonly TimeSpan topicMetadataRefreshInterval;

        private DateTime lastTopicMetadataRefeshTime = DateTime.MinValue;

        private HashSet<string> topicMetadataToRefresh = new HashSet<string>();  

        private Dictionary<string, int> sendPartitionPerTopicCache = new Dictionary<string, int>();

        private readonly ProducerStats producerStats;

        private readonly ProducerTopicStats producerTopicStats;

        public void Handle(IEnumerable<KeyedMessage<TKey, TValue>> events)
        {
            var serializedData = this.Serialize(events);

            foreach (var keyed in serializedData)
            {
                var dataSize = keyed.Message.PayloadSize;
                this.producerTopicStats.GetProducerTopicStats(keyed.Topic).ByteRate.Mark(dataSize);
                this.producerTopicStats.GetProducerAllTopicsStats().ByteRate.Mark(dataSize);
            }

            var outstandingProduceRequests = serializedData;
            var remainingRetries = this.config.MessageSendMaxRetries + 1;
            var correlationIdStart = this.correlationId.Get();

            Logger.DebugFormat("Handling {0} events", events.Count());

            while (remainingRetries > 0 && outstandingProduceRequests.Count() > 0)
            {
                foreach (var el in outstandingProduceRequests)
                {
                    topicMetadataToRefresh.Add(el.Topic);
                }
                if (topicMetadataRefreshInterval >= TimeSpan.MinValue
                    && (DateTime.Now - lastTopicMetadataRefeshTime) > topicMetadataRefreshInterval)
                {
                    try
                    {
                        brokerPartitionInfo.UpdateInfo(
                            new HashSet<string>(topicMetadataToRefresh), correlationId.GetAndIncrement());
                    }
                    catch
                    {
                    }
                    sendPartitionPerTopicCache.Clear();
                    topicMetadataToRefresh.Clear();
                    lastTopicMetadataRefeshTime = DateTime.Now;
                }

                outstandingProduceRequests = this.DispatchSerializedData(outstandingProduceRequests);
                if (outstandingProduceRequests.Count() > 0)
                {
                    Logger.InfoFormat("Back off for {0} ms before retrying send. Remaining retries = {1}", config.RetryBackoffMs, remainingRetries - 1);
                    // back off and update the topic metadata cache before attempting another send operation
                     Thread.Sleep(this.config.RetryBackoffMs);
                    try
                    {
                        brokerPartitionInfo.UpdateInfo(
                            new HashSet<string>(outstandingProduceRequests.Select(r => r.Topic)), correlationId.GetAndIncrement());
                        sendPartitionPerTopicCache.Clear();
                        remainingRetries -= 1;
                        producerStats.ResendRate.Mark();
                    }
                    catch
                    {
                    }
                }

                this.sendPartitionPerTopicCache.Clear();
                remainingRetries -= 1;
            }

            if (outstandingProduceRequests.Count() > 0)
            {
                producerStats.FailedSendRate.Mark();
                var correlationIdEnd = correlationId.Get();
                Logger.ErrorFormat("Failed to send requests for topics {0} with correlation ids in [{1}, {2}]", string.Join(",", outstandingProduceRequests.Select(r => r.Topic)), correlationIdStart, correlationIdEnd);

                throw new FailedToSendMessageException(
                    "Failed to send messages after " + this.config.MessageSendMaxRetries + " tries");
            }
        }

        private List<KeyedMessage<TKey, Message>> DispatchSerializedData(List<KeyedMessage<TKey, Message>> messages)
        {
            var partitionedData = this.PartitionAndCollate(messages);
            if (partitionedData == null)
            {
                return messages;
            }
            var failedProduceRequests = new List<KeyedMessage<TKey, Message>>();
            try
            {
                foreach (var keyValuePair in partitionedData)
                {
                    var brokerId = keyValuePair.Key;
                    var messagesPerBrokerMap = keyValuePair.Value;

                    if (Logger.IsDebugEnabled)
                    {
                        foreach (var partitionAndEvent in messagesPerBrokerMap)
                        {
                            Logger.DebugFormat("Handling event for Topic: {0}, Broker: {1}, Partitions: {2}", partitionAndEvent.Key, brokerId, string.Join(",", partitionAndEvent.Value));
                        }
                    }

                    var messageSetPerBroker = this.GroupMessagesToSet(messagesPerBrokerMap);
                    var failedTopicPartitions = this.Send(brokerId, messageSetPerBroker);
                    foreach (var topicPartiton in failedTopicPartitions)
                    {
                        List<KeyedMessage<TKey, Message>> data = null;
                        if (messagesPerBrokerMap.TryGetValue(topicPartiton, out data))
                        {
                            failedProduceRequests.AddRange(data);
                        }
                    }
                }
            }
            catch (Exception)
            {
                Logger.Error("Failed to send messages");
            }
            return failedProduceRequests;
        }

        private List<KeyedMessage<TKey, Message>> Serialize(IEnumerable<KeyedMessage<TKey, TValue>> events)
        {

            return events.Select(
                e =>
                    {
                        try
                        {
                            if (e.HasKey)
                            {
                                return new KeyedMessage<TKey, Message>(
                                    e.Topic,
                                    e.Key,
                                    e.PartKey,
                                    new Message(encoder.ToBytes(e.Message), keyEncoder.ToBytes(e.Key)));
                            }
                            else
                            {
                                return new KeyedMessage<TKey, Message>(
                                    e.Topic, e.Key, e.PartKey, new Message(encoder.ToBytes(e.Message)));
                            }
                        }
                        catch (Exception ex)
                        {
                            producerStats.SerializationErrorRate.Mark();
                            if (IsSync)
                            {
                                throw ex;
                            }
                            else
                            {
                                Logger.ErrorFormat("Error serializing message for topic {0}", e.Topic, ex);
                                return null;
                            }
                        }

                    }).ToList();
        }

        private IDictionary<int, Dictionary<TopicAndPartition, List<KeyedMessage<TKey, Message>>>> PartitionAndCollate(List<KeyedMessage<TKey, Message>> messages)
        {
            var ret = new Dictionary<int, Dictionary<TopicAndPartition, List<KeyedMessage<TKey, Message>>>>();
            try
            {


                foreach (var message in messages)
                {
                    var topicPartitionsList = this.GetPartitionListForTopic(message);
                    var partitionIndex = this.GetPartition(message.Topic, message.PartitionKey, topicPartitionsList);
                    var brokerPartition = topicPartitionsList.ElementAt(partitionIndex);

                    // postpone the failure until the send operation, so that requests for other brokers are handled correctly
                    var leaderBrokerId = brokerPartition.LeaderBrokerIdOpt ?? -1;

                    Dictionary<TopicAndPartition, List<KeyedMessage<TKey, Message>>> dataPerBroker = null;
                    if (ret.ContainsKey(leaderBrokerId))
                    {
                        dataPerBroker = ret[leaderBrokerId];
                    }
                    else
                    {
                        dataPerBroker = new Dictionary<TopicAndPartition, List<KeyedMessage<TKey, Message>>>();
                        ret[leaderBrokerId] = dataPerBroker;
                    }

                    var topicAndPartition = new TopicAndPartition(message.Topic, brokerPartition.PartitionId);
                    List<KeyedMessage<TKey, Message>> dataPerTopicPartition = null;
                    if (dataPerBroker.ContainsKey(topicAndPartition))
                    {
                        dataPerTopicPartition = dataPerBroker[topicAndPartition];
                    }
                    else
                    {
                        dataPerTopicPartition = new List<KeyedMessage<TKey, Message>>();
                        dataPerBroker[topicAndPartition] = dataPerTopicPartition;
                    }
                    dataPerTopicPartition.Add(message);
                }
                return ret;
            }
            catch (UnknownTopicOrPartitionException e)
            {
                Logger.Warn("Failed to collate messages by topic,partition due to: " + e.Message, e);
                return null;
            }
            catch (LeaderNotAvailableException e)
            {
                Logger.Warn("Failed to collate messages by topic,partition due to: " + e.Message, e);
                return null;
            }
            catch (Exception e)
            {
                Logger.Error("Failed to collate messages by topic, partition due to: " + e.Message, e);
                return null;
            }
        }

        private IList<PartitionAndLeader> GetPartitionListForTopic(KeyedMessage<TKey, Message> m)
        {
            var topicPartitionsList = this.brokerPartitionInfo.GetBrokerPartitionInfo(m.Topic, correlationId.GetAndIncrement());
            Logger.DebugFormat("Broker partitions registered for topic: {0} are {1}", m.Topic, string.Join(",", topicPartitionsList.Select(p => p.PartitionId.ToString())));
            var totalNumPartitions = topicPartitionsList.Count();
            if (totalNumPartitions == 0)
            {
                throw new NoBrokersForPartitionException("Partition key = " + m.Key);
            }
            return topicPartitionsList;
        }

        /// <summary>
        /// Retrieves the partition id and throws an UnknownTopicOrPartitionException if
        /// the value of partition is not between 0 and numPartitions-1
        /// </summary>
        /// <param name="key"></param>
        /// <param name="numPartitions"></param>
        /// <returns></returns>
        private int GetPartition(string topic, object key, IEnumerable<PartitionAndLeader> topicPartitionList)
        {
            var numPartitions = topicPartitionList.Count();
            if (numPartitions <= 0)
            {
                throw new UnknownTopicOrPartitionException(
                    string.Format("Topic {0} doesn't exists", topic));
            }
            int partition = 0;
            if (key == null)
            {
                // If the key is null, we don't really need a partitioner
                // So we look up in the send partition cache for the topic to decide the target partition
                if (sendPartitionPerTopicCache.ContainsKey(topic))
                {
                    partition = sendPartitionPerTopicCache[topic];
                }
                else
                {
                    var availablePartitons = topicPartitionList.Where(p => p.LeaderBrokerIdOpt.HasValue).ToList();
                    if (availablePartitons.Count() == 0)
                    {
                        throw new LeaderNotAvailableException(
                            string.Format("No leader for any partition in topic {0}", topic));
                    }
                    var index = random.Next(availablePartitons.Count());
                    var partitionId = availablePartitons[index].PartitionId;
                    this.sendPartitionPerTopicCache[topic] = partitionId;
                    partition = partitionId;
                }
            }
            else
            {
                partition = this.partitioner.Partition(key, numPartitions);
            }

            if (partition < 0 || partition >= numPartitions)
            {
                throw new UnknownTopicOrPartitionException(
                    string.Format("Invalid partition id : {0}. Valid values are in the range inclusive [0, {1}]",
                                  partition, (numPartitions - 1)));
            }
            Logger.DebugFormat("Assigning message of topic {0} and key {1} to a selected partition {2}", topic, (key == null) ? "[none]" : key.ToString(), partition);
            return partition;
        }

        /// <summary>
        /// Constructs and sends the produce request based on a map from (topic, partition) -> messages
        /// </summary>
        /// <param name="brokerId">brokerId the broker that will receive the request</param>
        /// <param name="messagesPerTopic"></param>
        /// <returns> the set (topic, partitions) messages which incurred an error sending or processing</returns>
        private List<TopicAndPartition> Send(int brokerId, IDictionary<TopicAndPartition, ByteBufferMessageSet> messagesPerTopic)
        {
            if (brokerId < 0)
            {
                Logger.WarnFormat("Failed to send Data since partitions {0} don't have a leader", string.Join(",", messagesPerTopic.Select(m => m.Key.Partiton)));
                return new List<TopicAndPartition>(messagesPerTopic.Keys);
            }
            if (messagesPerTopic.Count > 0)
            {
                var currentCorrelationId = correlationId.GetAndIncrement();
                var producerRequest = new ProducerRequest(currentCorrelationId, config.ClientId, config.RequestRequiredAcks, config.RequestTimeoutMs, messagesPerTopic);
                var failedTopicPartitions = new List<TopicAndPartition>();

                try {

                    var syncProducer = this.producerPool.GetProducer(brokerId);
                    Logger.DebugFormat("Producer sending messages with correlation id {0} for topics {1} to broker {2} on {3}:{4}",
                        currentCorrelationId, string.Join(",", messagesPerTopic.Keys), brokerId, syncProducer.Config.Host, syncProducer.Config.Port);

                    var response = syncProducer.Send(producerRequest);

                    if (response != null)
                    {
                        if (response.Status.Count() != producerRequest.Data.Count())
                        {
                            throw new KafkaException(
                                string.Format(
                                    "Incomplete response ({0}) for producer request ({1})", response, producerRequest));
                        }
                        if (Logger.IsDebugEnabled)
                        {
                            var successfullySentData = response.Status.Where(s => s.Value.Error == ErrorMapping.NoError).ToList();
                            foreach (var m in successfullySentData)
                            {
                                var iter = messagesPerTopic[m.Key].Iterator();
                                while (iter.HasNext())
                                {
                                    var message = iter.Next();
                                    Logger.DebugFormat(
                                    "Successfully sent messsage: {0}",
                                    message.Message.IsNull() ? 
                                    null : 
                                    Util.ReadString(message.Message.Payload));
                                }
                            }
                        }

                        var failedPartitionsAndStatus = response.Status.Where(s => s.Value.Error != ErrorMapping.NoError).ToList();
                        failedTopicPartitions =
                            failedPartitionsAndStatus.Select(partitionStatus => partitionStatus.Key).ToList();
                        if (failedTopicPartitions.Any())
                        {
                            var errorString = string.Join(
                                ",",
                                failedPartitionsAndStatus.OrderBy(x => x.Key.Topic)
                                                         .ThenBy(x => x.Key.Partiton)
                                                         .Select(
                                                             kvp =>
                                                                 { 
                                                                     var topicAndPartiton = kvp.Key;
                                                                     var status = kvp.Value;
                                                                     return topicAndPartiton.ToString() + ": "
                                                                            + ErrorMapping.ExceptionFor(status.Error)
                                                                                          .GetType()
                                                                                          .Name;
                                                                 }));
                            Logger.WarnFormat("Produce request with correlation id {0} failed due to {1}", currentCorrelationId, errorString);
                        }

                        return failedTopicPartitions;
                    }
                    else
                    {
                        return new List<TopicAndPartition>();
                    }
                } 
                catch (Exception e) 
                {
                            Logger.Warn(
                                string.Format(
                                    "Failed to send producer request with correlation id {0} to broker {1} with Data for partitions {2}",
                                    currentCorrelationId,
                                    brokerId,
                                    string.Join(",", messagesPerTopic.Select(m => m.Key))),
                                e);
                            return new List<TopicAndPartition>(messagesPerTopic.Keys);
                }
            }
            else
            {
                return new List<TopicAndPartition>();
            }
           
        }

        private IDictionary<TopicAndPartition, ByteBufferMessageSet> GroupMessagesToSet(IDictionary<TopicAndPartition, List<KeyedMessage<TKey, Message>>> eventsPerTopicAndPartition)
        {
            /** enforce the compressed.topics config here.
              *  If the compression codec is anything other than NoCompressionCodec,
              *    Enable compression only for specified topics if any
              *    If the list of compressed topics is empty, then enable the specified compression codec for all topics
              *  If the compression codec is NoCompressionCodec, compression is disabled for all topics
              */

            IDictionary<TopicAndPartition, ByteBufferMessageSet> messagesPerTopicPartition =
                new Dictionary<TopicAndPartition, ByteBufferMessageSet>();
            foreach (var keyValuePair in eventsPerTopicAndPartition)
            {
                var topicAndPartition = keyValuePair.Key;
                var messages = keyValuePair.Value.Select(m => m.Message).ToList();
                switch (this.config.CompressionCodec)
                {
                    case CompressionCodecs.NoCompressionCodec:
                        Logger.DebugFormat("Sending {0} messages with no compression to {1}", messages.Count(), topicAndPartition);
                        messagesPerTopicPartition.Add(new KeyValuePair<TopicAndPartition, ByteBufferMessageSet>(
                            topicAndPartition,
                            new ByteBufferMessageSet(
                                CompressionCodecs.NoCompressionCodec,
                                messages)));
                        break;
                    default:
                        if (this.config.CompressedTopics.Count() == 0)
                        {
                            messagesPerTopicPartition.Add(new KeyValuePair<TopicAndPartition, ByteBufferMessageSet>(topicAndPartition, new ByteBufferMessageSet(this.config.CompressionCodec, messages)));

                        }
                        else
                        {
                            if (this.config.CompressedTopics.Contains(topicAndPartition.Topic))
                            {
                                messagesPerTopicPartition.Add(new KeyValuePair<TopicAndPartition, ByteBufferMessageSet>(topicAndPartition, new ByteBufferMessageSet(this.config.CompressionCodec, messages)));
                            }
                            else
                            {
                                messagesPerTopicPartition.Add(new KeyValuePair<TopicAndPartition, ByteBufferMessageSet>(topicAndPartition, new ByteBufferMessageSet(CompressionCodecs.NoCompressionCodec, messages)));
                            }
                        }
                        break;
                }

            }
            return messagesPerTopicPartition;
        }

        public void Dispose()
        {
            if (this.producerPool != null)
            {
                producerPool.Dispose();
            }
        }

    }
}