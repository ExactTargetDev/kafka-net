/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Reflection;
using System.Threading;
using Kafka.Client.Exceptions;
using Kafka.Client.Messages;
using Kafka.Client.Producers.Partitioning;
using Kafka.Client.Requests;
using Kafka.Client.Serialization;
using Kafka.Client.Utils;
using log4net;

namespace Kafka.Client.Producers.Async
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Kafka.Client.Cfg;
using Kafka.Client.Cluster;

    internal class DefaultCallbackHandler<K, V> : ICallbackHandler<K, V>
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private ProducerConfiguration producerConfig;

        private IPartitioner<K> partitioner;

        private IEncoder<V> encoder;

        private ProducerPool producerPool;

        private BrokerPartitionInfo brokerPartitionInfo;

        private object myLock = new object();

        Random random = new Random();

        public DefaultCallbackHandler(ProducerConfiguration config, IPartitioner<K> partitioner, IEncoder<V> encoder, ProducerPool producerPool)
        {
            this.producerConfig = config;
            this.partitioner = partitioner;
            this.encoder = encoder;
            this.producerPool = producerPool;
            this.brokerPartitionInfo = new BrokerPartitionInfo(this.producerPool);
            this.producerPool.AddProducers(this.producerConfig);
        }

        public void Handle(IEnumerable<ProducerData<K, V>> events)
        {
            lock (myLock)
            {
                var serializedData = this.Serialize(events);
                var outstandingProduceRequests = serializedData;
                var remainingRetries = this.producerConfig.ProducerRetries;
                while (remainingRetries > 0 && outstandingProduceRequests.Count() > 0)
                {
                    var currentOutstandingRequests = this.DispatchSerializedData(outstandingProduceRequests);
                    outstandingProduceRequests = currentOutstandingRequests;
                    // back off and update the topic metadata cache before attempting another send operation
                    Thread.Sleep(this.producerConfig.ProducerRetryBackoffMiliseconds);
                    this.brokerPartitionInfo.UpdateInfo();
                    remainingRetries -= 1;
                }
            }
        }

        private IEnumerable<ProducerData<K, Message>> Serialize(IEnumerable<ProducerData<K, V>> events)
        {
            return events.Select(
                e => new ProducerData<K, Message>(e.Topic, e.Key, e.Data.Select(m => this.encoder.ToMessage(m))));
        }

        private IEnumerable<ProducerData<K,Message>> DispatchSerializedData(IEnumerable<ProducerData<K,Message>> messages)
        {
            var partitionedData = this.PartitionAndCollate(messages);
            var failedProduceRequests = new List<ProducerData<K, Message>>();
            try
            {
                foreach (KeyValuePair<int, Dictionary<Tuple<string, int>, IEnumerable<ProducerData<K, Message>>>> keyValuePair in partitionedData)
                {
                    var brokerId = keyValuePair.Key;
                    var eventsPerBrokerMap = keyValuePair.Value;
                    var messageSetPerBroker = this.GroupMessagesToSet(eventsPerBrokerMap);
                    if (brokerId < 0 || !this.Send(brokerId, messageSetPerBroker))
                    {
                        failedProduceRequests.AddRange(eventsPerBrokerMap.SelectMany(x => x.Value));
                    }
                }
            }
            catch (Exception)
            {
                Logger.Error("Failed to send messages");
            }
            return failedProduceRequests;
        }

        private bool Send(int brokerId, IDictionary<Tuple<string, int>, BufferedMessageSet> messagesPerTopic)
        {
            try
            {
                if (brokerId < 0)
                {
                    throw new NoLoaderForPartitionException(
                        string.Format("No loader for some partition(s) on broker {0}", brokerId));
                }
                if (messagesPerTopic.Count > 0)
                {
                    var topics = new Dictionary<string, List<PartitionData>>();
                    foreach (KeyValuePair<Tuple<string, int>, BufferedMessageSet> keyValuePair in messagesPerTopic)
                    {
                        var topicName = keyValuePair.Key.Item1;
                        var partitionId = keyValuePair.Key.Item2;
                        var messagesSet = keyValuePair.Value;
                        if (topics.ContainsKey(topicName))
                        {
                            Logger.DebugFormat("Found " + topicName );
                        }
                        else
                        {
                            topics.Add(topicName, new List<PartitionData>()); //create a new list for this topic
                        }
                        topics[topicName].Add(new PartitionData(partitionId, messagesSet));
                    }
                    var topicData = topics.Select(kv => new TopicData(kv.Key, kv.Value));
                    var producerRequest = new ProducerRequest(this.producerConfig.CorrelationId,
                                                              this.producerConfig.ClientId,
                                                              this.producerConfig.RequiredAcks,
                                                              this.producerConfig.AckTimeout, topicData);
                    var syncProducer = this.producerPool.GetProducer(brokerId);
                    var response = syncProducer.Send(producerRequest);
                    // TODO: possibly send response to response callback handler
                    Logger.DebugFormat("Kafka producer sent messages for topics {0} to broker {1} on {2}:{3}",
                                       messagesPerTopic, brokerId, syncProducer.Config.Host, syncProducer.Config.Port);
                }
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private IDictionary<int, Dictionary<Tuple<string, int>, IEnumerable<ProducerData<K, Message>>>> PartitionAndCollate(IEnumerable<ProducerData<K, Message>> events)
        {
            var ret = new Dictionary<int, Dictionary<Tuple<string, int>, IEnumerable<ProducerData<K, Message>>>>();
            foreach (var eventItem in events)
            {
                var topicPartitionsList = this.GetPartitionListForTopic(eventItem);
                var totalNumPartitions = topicPartitionsList.Count();
                var partitionIndex = this.GetPartition(eventItem.Key, totalNumPartitions);
                var brokerPartition = topicPartitionsList.ElementAt(partitionIndex);
                var leaderBrokerId = brokerPartition.Leader != null ? brokerPartition.Leader.BrokerId : -1; // postpone the failure until the send operation, so that requests for other brokers are handled correctly

                Dictionary<Tuple<string, int>, IEnumerable<ProducerData<K, Message>>> dataPerBroker = null;
                if (ret.ContainsKey(leaderBrokerId))
                {
                    dataPerBroker = ret[leaderBrokerId];
                }
                else
                {
                    dataPerBroker = new Dictionary<Tuple<string, int>, IEnumerable<ProducerData<K, Message>>>();
                    ret.Add(leaderBrokerId, dataPerBroker);
                }

                var topicAndPartition = new Tuple<string, int>(eventItem.Topic, brokerPartition.PartId);
                List<ProducerData<K, Message>> dataPerTopicPartition = null;
                if (dataPerBroker.ContainsKey(topicAndPartition))
                {
                    dataPerTopicPartition = new List<ProducerData<K, Message>>(dataPerBroker[topicAndPartition]);
                }
                else
                {
                    dataPerTopicPartition = new List<ProducerData<K, Message>>();
                    dataPerBroker.Add(topicAndPartition, dataPerTopicPartition);
                }
                dataPerTopicPartition.Add(eventItem);
            }
            return ret;
        }

        private IEnumerable<Partition> GetPartitionListForTopic(ProducerData<K, Message> pd)
        {
            Logger.DebugFormat("Getting the number of broker partitions registered for topic: {0}", pd.Topic);
            var topicPartitionsList = this.brokerPartitionInfo.GetBrokerPartitionInfo(pd.Topic);
            Logger.DebugFormat("Broker partitions registered for topic: {0} are {1}", pd.Topic, string.Join(",", topicPartitionsList.Select(p => p.PartId.ToString())));
            var totalNumPartitions = topicPartitionsList.Count();
            if (totalNumPartitions == 0)
            {
                throw new NoBrokersForPartitionException("Partition = " + pd.Key);
            }
            return topicPartitionsList;
        }

        private int GetPartition(K key, int numPartitions)
        {
            if (numPartitions <= 0)
            {
                throw new InvalidPartitionException(
                    string.Format("Invalid number of partitions: {0}. Valid values are > 0", numPartitions));
            }
            var partition = key == null ? random.Next(numPartitions) : this.partitioner.Partition(key, numPartitions);
            if (partition < 0 || partition >= numPartitions)
            {
                throw new InvalidPartitionException(
                    string.Format("Invalid partition id : {0}. Valid values are in the range inclusive [0, {1}]",
                                  partition, (numPartitions - 1)));
            }
            return partition;
        }

        private IDictionary<Tuple<string, int>, BufferedMessageSet> GroupMessagesToSet(IDictionary<Tuple<string, int>, IEnumerable<ProducerData<K, Message>>> eventsPerTopicAndPartition)
        {
            IDictionary<Tuple<string, int>, BufferedMessageSet> messagesPerTopicPartition =
                new Dictionary<Tuple<string, int>, BufferedMessageSet>();
            foreach (KeyValuePair<Tuple<string, int>, IEnumerable<ProducerData<K, Message>>> keyValuePair in eventsPerTopicAndPartition)
            {
                var topicAndPartition = keyValuePair.Key;
                var produceData = keyValuePair.Value;
                var messages = new List<Message>();
                produceData.ForEach(p => messages.AddRange(p.Data));
                switch(this.producerConfig.CompressionCodec)
                {
                    case CompressionCodecs.NoCompressionCodec:
                        messagesPerTopicPartition.Add(new KeyValuePair
                            <Tuple<string, int>, BufferedMessageSet>(
                            topicAndPartition,
                            new BufferedMessageSet(
                                CompressionCodecs.NoCompressionCodec,
                                messages)));
                        break;
                    default:
                        if(this.producerConfig.CompressedTopics.Count() == 0)
                        {
                                messagesPerTopicPartition.Add(new KeyValuePair<Tuple<string, int>, BufferedMessageSet>(topicAndPartition, new BufferedMessageSet(this.producerConfig.CompressionCodec, messages)));

                        } else
                        {
                            if (this.producerConfig.CompressedTopics.Contains(topicAndPartition.Item1))
                            {
                                messagesPerTopicPartition.Add(new KeyValuePair<Tuple<string, int>, BufferedMessageSet>(topicAndPartition, new BufferedMessageSet(this.producerConfig.CompressionCodec, messages)));
                            }
                            else
                            {
                                messagesPerTopicPartition.Add(new KeyValuePair<Tuple<string, int>, BufferedMessageSet>(topicAndPartition, new BufferedMessageSet(CompressionCodecs.NoCompressionCodec, messages)));
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
                this.producerPool.Dispose();
            }
        }
    }
}
