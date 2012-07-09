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

using System.Configuration;
using Kafka.Client.Requests;

namespace Kafka.Client.ZooKeeperIntegration.Listeners
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using System.Threading;
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Consumers;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Utils;
    using Kafka.Client.ZooKeeperIntegration.Events;
    using log4net;
    using ZooKeeperNet;

    internal class ZKRebalancerListener<TData> : IZooKeeperChildListener
    {
        private IDictionary<string, IList<string>> oldPartitionsPerTopicMap = new Dictionary<string, IList<string>>();

        private IDictionary<string, IList<string>> oldConsumersPerTopicMap = new Dictionary<string, IList<string>>();

        private IDictionary<string, IDictionary<int, PartitionTopicInfo>> topicRegistry;

        private readonly IDictionary<Tuple<string, string>, BlockingCollection<FetchedDataChunk>> queues;

        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly string consumerIdString;

        private readonly object syncLock;

        private readonly ConsumerConfiguration config;

        private readonly IZooKeeperClient zkClient;

        private readonly ZKGroupDirs dirs;

        private readonly Fetcher fetcher;

        private readonly ZookeeperConsumerConnector zkConsumerConnector;

        private readonly IDictionary<string, IList<KafkaMessageStream<TData>>> kafkaMessageStreams;

        internal ZKRebalancerListener(
            ConsumerConfiguration config,
            string consumerIdString,
            IDictionary<string, IDictionary<int, PartitionTopicInfo>> topicRegistry,
            IZooKeeperClient zkClient,
            ZookeeperConsumerConnector zkConsumerConnector,
            IDictionary<Tuple<string, string>, BlockingCollection<FetchedDataChunk>> queues,
            Fetcher fetcher,
            object syncLock,
            IDictionary<string, IList<KafkaMessageStream<TData>>> kafkaMessageStreams)
        {
            this.syncLock = syncLock;
            this.consumerIdString = consumerIdString;
            this.config = config;
            this.topicRegistry = topicRegistry;
            this.zkClient = zkClient;
            this.dirs = new ZKGroupDirs(config.GroupId);
            this.zkConsumerConnector = zkConsumerConnector;
            this.queues = queues;
            this.fetcher = fetcher;
            this.kafkaMessageStreams = kafkaMessageStreams;
        }

        public void SyncedRebalance()
        {
            lock (this.syncLock)
            {
                for (int i = 0; i < ZookeeperConsumerConnector.MaxNRetries; i++)
                {
                    Logger.InfoFormat(CultureInfo.CurrentCulture, "begin rebalancing consumer {0} try #{1}", consumerIdString, i);
                    bool done = false;
                    var cluster = new Cluster(zkClient);
                    try
                    {
                        done = this.Rebalance(cluster);
                    }
                    catch (Exception ex)
                    {
                        Logger.InfoFormat(CultureInfo.CurrentCulture, "exception during rebalance {0}", ex);
                        done = false;
                    }

                    Logger.InfoFormat(CultureInfo.CurrentCulture, "end rebalancing consumer {0} try #{1}", consumerIdString, i);
                    if (done)
                    {
                        return;
                    }
                    else
                    {
                        Logger.Info(
                            "Rebalancing attempt failed. Clearing the cache before the next rebalancing operation is triggered");
                        this.oldConsumersPerTopicMap.Clear();
                        this.oldPartitionsPerTopicMap.Clear();
                    }
                    //commit offsets
                    zkConsumerConnector.CommitOffsets();
                    //stop all fetchers and clear all the queues to avoid data duplication
                    CloseFetchersForQueues(cluster, queues.Select(q => q.Value), this.kafkaMessageStreams, this.zkConsumerConnector);
                    //release all partitions, reset state and retry
                    ReleasePartitionOwnership();
                    Thread.Sleep(config.ZooKeeper.ZkSyncTimeMs);
                }
            }

            throw new ZKRebalancerException(string.Format(CultureInfo.CurrentCulture, "{0} can't rebalance after {1} retries", this.consumerIdString, ZookeeperConsumerConnector.MaxNRetries));
        }

        /// <summary>
        /// Called when the children of the given path changed
        /// </summary>
        /// <param name="args">The <see cref="Kafka.Client.ZooKeeperIntegration.Events.ZooKeeperChildChangedEventArgs"/> instance containing the event data
        /// as parent path and children (null if parent was deleted).
        /// </param>
        /// <remarks> 
        /// http://zookeeper.wiki.sourceforge.net/ZooKeeperWatches
        /// </remarks>
        public void HandleChildChange(ZooKeeperChildChangedEventArgs args)
        {
            Guard.NotNull(args, "args");
            Guard.NotNullNorEmpty(args.Path, "args.Path");
            Guard.NotNull(args.Children, "args.Children");
            SyncedRebalance();
        }

        /// <summary>
        /// Resets the state of listener.
        /// </summary>
        public void ResetState()
        {
            zkClient.SlimLock.EnterWriteLock();
            try
            {
                this.topicRegistry.Clear();
            }
            finally
            {
                zkClient.SlimLock.ExitWriteLock();
            }
            this.oldConsumersPerTopicMap.Clear();
            this.oldPartitionsPerTopicMap.Clear();
        }

        private bool Rebalance(Cluster cluster)
        {
            var myTopicThresdIdsMap = this.GetTopicCount(this.consumerIdString).GetConsumerThreadIdsPerTopic();
            var consumersPerTopicMap = this.GetConsumersPerTopic(this.config.GroupId);
            var partitionsPerTopicMap = ZkUtils.GetPartitionsForTopics(this.zkClient, myTopicThresdIdsMap.Keys);
            var relevantTopicThreadIdsMap = GetRelevantTopicMap(
                myTopicThresdIdsMap,
                partitionsPerTopicMap,
                this.oldPartitionsPerTopicMap,
                consumersPerTopicMap,
                this.oldConsumersPerTopicMap);
            if (relevantTopicThreadIdsMap.Count <= 0)
            {
                Logger.InfoFormat(CultureInfo.CurrentCulture, "Consumer {0} with {1} doesn't need to rebalance.", this.consumerIdString, consumersPerTopicMap);
                return true;
            }

            this.CloseFetchers(cluster, relevantTopicThreadIdsMap, this.zkConsumerConnector);

            this.ReleasePartitionOwnership();

            var partitionOwnershipDecision = new Dictionary<Tuple<string, string>, string>();

            foreach (var item in relevantTopicThreadIdsMap)
            {
                var topic = item.Key;
                var consumerThreadIdSet = item.Value;

                topicRegistry.Remove(topic);
                topicRegistry.Add(topic, new ConcurrentDictionary<int, PartitionTopicInfo>());
                //currentTopicRegistry.GetOrAdd(item.Key, new ConcurrentDictionary<Partition, PartitionTopicInfo>());

                var topicDirs = new ZKGroupTopicDirs(config.GroupId, topic);
                var curConsumers = consumersPerTopicMap[topic];
                var curPartitions = new List<string>(partitionsPerTopicMap[topic]);

                var numberOfPartsPerConsumer = curPartitions.Count / curConsumers.Count;
                var numberOfConsumersWithExtraPart = curPartitions.Count % curConsumers.Count;

                Logger.InfoFormat(
                    CultureInfo.CurrentCulture,
                    "Consumer {0} rebalancing the following partitions: {1} for topic {2} with consumers: {3}",
                    this.consumerIdString,
                    string.Join(",", curPartitions),
                    topic,
                    string.Join(",", curConsumers));

                foreach (string consumerThreadId in consumerThreadIdSet)
                {
                    var myConsumerPosition = curConsumers.IndexOf(consumerThreadId);
                    if (myConsumerPosition < 0)
                    {
                        continue;
                    }

                    var startPart = (numberOfPartsPerConsumer * myConsumerPosition) +
                                    Math.Min(myConsumerPosition, numberOfConsumersWithExtraPart);
                    var numberOfParts = numberOfPartsPerConsumer + (myConsumerPosition + 1 > numberOfConsumersWithExtraPart ? 0 : 1);

                    if (numberOfParts <= 0)
                    {
                        Logger.WarnFormat(CultureInfo.CurrentCulture, "No broker partitions consumed by consumer thread {0} for topic {1}", consumerThreadId, item.Key);
                    }
                    else
                    {
                        for (int i = startPart; i < startPart + numberOfParts; i++)
                        {
                            var partition = curPartitions[i];
                            Logger.InfoFormat(CultureInfo.CurrentCulture, "{0} attempting to claim partition {1}", consumerThreadId, partition);
                            var ownPartition = this.ProcessPartition(topicDirs, partition, topic, consumerThreadId);
                            if (!ownPartition)
                            {
                                return false;
                            }
                            else //record the partition ownership decision
                            {
                                partitionOwnershipDecision.Add(new Tuple<string, string>(topic, partition), consumerThreadId);
                            }
                        }
                    }
                }
            }

            if (ReflectPartitionOwnershipDecision(partitionOwnershipDecision))
            {
                zkClient.SlimLock.EnterWriteLock();
                try
                {
                    this.UpdateFetcher(cluster);
                }
                finally
                {
                    zkClient.SlimLock.ExitWriteLock();
                }
                this.oldPartitionsPerTopicMap = partitionsPerTopicMap;
                this.oldConsumersPerTopicMap = consumersPerTopicMap;
                return true;
            }
            else
            {
                return false;
            }
        }

        private bool ProcessPartition(ZKGroupTopicDirs topicDirs, string partition, string topic, string consumerThreadId)
        {
            var partitionOwnerPath = topicDirs.ConsumerOwnerDir + "/" + partition;
            //check if some other consumer owns this partition at this time
            var currentPartitionOwner = zkClient.ReadData<string>(partitionOwnerPath, true);
            if (currentPartitionOwner != null)
            {
                if (currentPartitionOwner.Equals(consumerThreadId))
                {
                    Logger.InfoFormat("{0} exists with value {1} during connection loss; this is ok", partitionOwnerPath, currentPartitionOwner);
                    AddPartitionTopicInfo(topicDirs, partition, topic, consumerThreadId);
                    return true;
                }
                else
                {
                    Logger.InfoFormat("{0} exists with value {1}", partitionOwnerPath, currentPartitionOwner);
                    return false;
                }
            }
            else
            {
                AddPartitionTopicInfo(topicDirs, partition, topic, consumerThreadId);
                return true;
            }
        }

        private bool ReflectPartitionOwnershipDecision(Dictionary<Tuple<string, string>, string> partitionOwnershipDecision)
        {
            //var successfullyOwnedPartitions = new List<Tuple<string, string>>();
            var partitionOwnershipSuccessful = new List<bool>();
            foreach (var partitionOwner in partitionOwnershipDecision)
            {
                var topic = partitionOwner.Key.Item1;
                var partition = partitionOwner.Key.Item2;
                var consumerThreadId = partitionOwner.Value;
                var topicDirs = new ZKGroupTopicDirs(config.GroupId, topic);
                var partitionOwnerPath = topicDirs.ConsumerOwnerDir + "/" + partition;
                try
                {
                    ZkUtils.CreateEphemeralPathExpectConflict(zkClient, partitionOwnerPath, consumerThreadId);
                    Logger.InfoFormat("{0} successfully owned partition {1} for topic {2}", consumerThreadId, partition, topic);
                    //successfullyOwnedPartitions.Add(new Tuple<string, string>(topic, partition));
                    partitionOwnershipSuccessful.Add(true);
                }
                catch (KeeperException.NodeExistsException)
                {
                    Logger.InfoFormat("waiting for the partition owner to be deleted: {0}", partition);
                    partitionOwnershipSuccessful.Add(false);
                }
            }
            var hasPartitionOwnershipFailed = partitionOwnershipSuccessful.Contains(false);
            return !hasPartitionOwnershipFailed;
        }

        private void CloseFetchers(Cluster cluster, IDictionary<string, IList<string>> relevantTopicThreadIdsMap, ZookeeperConsumerConnector zkConsumerConnector)
        {
            var queuesToBeCleared = queues.Where(q => relevantTopicThreadIdsMap.ContainsKey(q.Key.Item1)).Select(q => q.Value).ToList();
            CloseFetchersForQueues(cluster, queuesToBeCleared, this.kafkaMessageStreams, zkConsumerConnector);
        }

        private void CloseFetchersForQueues(Cluster cluster, IEnumerable<BlockingCollection<FetchedDataChunk>> queuesToBeCleared, IDictionary<string, IList<KafkaMessageStream<TData>>> kafkaMessageStreams, ZookeeperConsumerConnector zkConsumerConnector)
        {
            if (this.fetcher != null)
            {
                var allPartitionInfos = new List<PartitionTopicInfo>();
                foreach (var item in this.topicRegistry.Values)
                {
                    foreach (var partitionTopicInfo in item.Values)
                    {
                        allPartitionInfos.Add(partitionTopicInfo);
                    }
                }
                fetcher.Shutdown();
                fetcher.ClearFetcherQueues(allPartitionInfos, cluster, queuesToBeCleared, kafkaMessageStreams);
                Logger.Info("Committing all offsets after clearing the fetcher queues");
                zkConsumerConnector.CommitOffsets();
            }
        }

        private void UpdateFetcher(Cluster cluster)
        {
            var allPartitionInfos = new List<PartitionTopicInfo>();
            foreach (var partitionInfos in this.topicRegistry.Values)
            {
                foreach (var partition in partitionInfos.Values)
                {
                    allPartitionInfos.Add(partition);
                }
            }
            Logger.InfoFormat("Consumer {0} selected partitions : {1}", consumerIdString,
                              string.Join(",", allPartitionInfos.OrderBy(x => x.PartitionId).Select(y => y.ToString())));
            if (this.fetcher != null)
            {
                this.fetcher.InitConnections(allPartitionInfos, cluster);
            }
        }

        private void AddPartitionTopicInfo(ZKGroupTopicDirs topicDirs, string partition, string topic, string consumerThreadId)
        {
            var partitionId = int.Parse(partition);
            var partTopicInfoMap = this.topicRegistry[topic];

            //find the leader for this partition
            var leaderOpt = ZkUtils.GetLeaderForPartition(this.zkClient, topic, partitionId);
            if (!leaderOpt.HasValue)
            {
                throw new NoBrokersForPartitionException(string.Format("No leader available for partitions {0} on topic {1}", partition, topic));
            }
            else
            {
                Logger.DebugFormat("Leader for partition {0} for topic {1} is {2}", partition, topic, leaderOpt.Value);
            }
            var leader = leaderOpt.Value;
            var znode = topicDirs.ConsumerOffsetDir + "/" + partition;
            var offsetString = this.zkClient.ReadData<string>(znode, true);

            //if first time starting a consumer, set the initial offset based on the config
            long offset = 0;
            if (offsetString == null)
            {
                switch (config.AutoOffsetReset)
                {
                    case OffsetRequest.SmallestTime:
                        offset = this.EarliestOrLatestOffset(topic, leader, partitionId, OffsetRequest.EarliestTime);
                        break;
                    case OffsetRequest.LargestTime:
                        offset = this.EarliestOrLatestOffset(topic, leader, partitionId, OffsetRequest.LatestTime);
                        break;
                    default:
                        throw new ConfigurationErrorsException("Wrong value in autoOffsetReset in ConsumerConfig");
                }
            }
            else
            {
                offset = long.Parse(offsetString);
            }

            var queue = this.queues[new Tuple<string, string>(topic, consumerThreadId)];
            var partTopicInfo = new PartitionTopicInfo(
                topic,
                leader,
                partitionId,
                queue,
                offset,
                offset,
                this.config.FetchSize);
            partTopicInfoMap.Add(partitionId, partTopicInfo);
            Logger.DebugFormat(CultureInfo.CurrentCulture, "{0} selected new offset {1}", partTopicInfo, offset);
        }

        private long EarliestOrLatestOffset(string topic, int brokerId, int partitionId, long earliestIoLatest)
        {
            Consumer consumer = null;
            long producedOffset = -1;
            try
            {
                var cluster = new Cluster(this.zkClient);
                var broker = cluster.GetBroker(brokerId);
                if (broker == null)
                {
                    throw new IllegalStateException(string.Format("Broker {0} is unavailable. Cannot issue GetOffsetsBefore request", brokerId));
                }
                consumer = new Consumer(this.config, broker.Host, broker.Port);
                var offsets = consumer.GetOffsetsBefore(new OffsetRequest(topic, partitionId, earliestIoLatest, 1));
                producedOffset = offsets[0];
            }
            catch (Exception ex)
            {
                Logger.Error("error in EarliestOrLatestOffset()", ex);
            }
            return producedOffset;
        }

        private void ReleasePartitionOwnership()
        {
            zkClient.SlimLock.EnterWriteLock();
            try
            {
                Logger.Info("Releasing partition ownership");
                foreach (KeyValuePair<string, IDictionary<int, PartitionTopicInfo>> item in topicRegistry)
                {
                    var topic = item.Key;
                    foreach (var partition in item.Value.Keys)
                    {
                        var partitionOwnerPath = ZkUtils.GetConsumerPartitionOwnerPath(this.config.GroupId, topic,
                                                                                       partition.ToString());
                        ZkUtils.DeletePath(zkClient, partitionOwnerPath);
                        Logger.DebugFormat("Consumer {0} releasing {1}", consumerIdString, partitionOwnerPath);
                    }
                }
            }
            finally
            {
                zkClient.SlimLock.ExitWriteLock();
            }
        }

        private TopicCount GetTopicCount(string consumerId)
        {
            var topicCountJson = this.zkClient.ReadData<string>(this.dirs.ConsumerRegistryDir + "/" + consumerId);
            return TopicCount.ConstructTopicCount(consumerId, topicCountJson);
        }

        private IDictionary<string, IList<string>> GetConsumersPerTopic(string group)
        {
            var consumers = this.zkClient.GetChildrenParentMayNotExist(this.dirs.ConsumerRegistryDir);
            var consumersPerTopicMap = new Dictionary<string, IList<string>>();
            foreach (var consumer in consumers)
            {
                TopicCount topicCount = GetTopicCount(consumer);
                foreach (KeyValuePair<string, IList<string>> consumerThread in topicCount.GetConsumerThreadIdsPerTopic())
                {
                    foreach (string consumerThreadId in consumerThread.Value)
                    {
                        if (!consumersPerTopicMap.ContainsKey(consumerThread.Key))
                        {
                            consumersPerTopicMap.Add(consumerThread.Key, new List<string> { consumerThreadId });
                        }
                        else
                        {
                            consumersPerTopicMap[consumerThread.Key].Add(consumerThreadId);
                        }
                    }
                }
            }

            foreach (KeyValuePair<string, IList<string>> item in consumersPerTopicMap)
            {
                item.Value.ToList().Sort();
            }

            return consumersPerTopicMap;
        }

        private static IDictionary<string, IList<string>> GetRelevantTopicMap(
            IDictionary<string, IList<string>> myTopicThreadIdsMap,
            IDictionary<string, IList<string>> newPartMap,
            IDictionary<string, IList<string>> oldPartMap,
            IDictionary<string, IList<string>> newConsumerMap,
            IDictionary<string, IList<string>> oldConsumerMap)
        {
            var relevantTopicThreadIdsMap = new Dictionary<string, IList<string>>();
            foreach (var myMap in myTopicThreadIdsMap)
            {
                var oldPartValue = oldPartMap.ContainsKey(myMap.Key) ? oldPartMap[myMap.Key] : null;
                var newPartValue = newPartMap.ContainsKey(myMap.Key) ? newPartMap[myMap.Key] : null;
                var oldConsumerValue = oldConsumerMap.ContainsKey(myMap.Key) ? oldConsumerMap[myMap.Key] : null;
                var newConsumerValue = newConsumerMap.ContainsKey(myMap.Key) ? newConsumerMap[myMap.Key] : null;
                if (oldPartValue != newPartValue || oldConsumerValue != newConsumerValue)
                {
                    relevantTopicThreadIdsMap.Add(myMap.Key, myMap.Value);
                }
            }

            return relevantTopicThreadIdsMap;
        }
    }
}
