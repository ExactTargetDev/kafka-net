namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics.Contracts;
    using System.Globalization;
    using System.Net;
    using System.Reflection;
    using System.Threading;

    using Kafka.Client.Clusters;
    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Metrics;
    using Kafka.Client.Serializers;
    using Kafka.Client.Utils;
    using Kafka.Client.ZKClient;
    using Kafka.Client.ZKClient.Exceptions;

    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    using Spring.Threading.Locks;

    using ZooKeeperNet;

    using log4net;

    using System.Linq;

    using Kafka.Client.Extensions;

    /// <summary>
    /// * This class handles the consumers interaction with zookeeper
    /// 
    ///  Directories:
    ///  1. Consumer id registry:
    ///  /consumers/[group_id]/ids[consumer_id] -> topic1,...topicN
    ///  A consumer has a unique consumer id within a consumer group. A consumer registers its id as an ephemeral znode
    ///  and puts all topics that it subscribes to as the value of the znode. The znode is deleted when the client is gone.
    ///  A consumer subscribes to event changes of the consumer id registry within its group.
    /// 
    ///  The consumer id is picked up from configuration, instead of the sequential id assigned by ZK. Generated sequential
    ///  ids are hard to recover during temporary connection loss to ZK, since it's difficult for the client to figure out
    ///  whether the creation of a sequential znode has succeeded or not. More details can be found at
    ///  (http://wiki.apache.org/hadoop/ZooKeeper/ErrorHandling)
    /// 
    ///  2. Broker node registry:
    ///  /brokers/[0...N] --> { "host" : "host:port",
    ///                         "topics" : {"topic1": ["partition1" ... "partitionN"], ...,
    ///                                     "topicN": ["partition1" ... "partitionN"] } }
    ///  This is a list of all present broker brokers. A unique logical node id is configured on each broker node. A broker
    ///  node registers itself on start-up and creates a znode with the logical node id under /brokers. The value of the znode
    ///  is a JSON String that contains (1) the host name and the port the broker is listening to, (2) a list of topics that
    ///  the broker serves, (3) a list of logical partitions assigned to each topic on the broker.
    ///  A consumer subscribes to event changes of the broker node registry.
    /// 
    ///  3. Partition owner registry:
    ///  /consumers/[group_id]/owner/[topic]/[broker_id-partition_id] --> consumer_node_id
    ///  This stores the mapping before broker partitions and consumers. Each partition is owned by a unique consumer
    ///  within a consumer group. The mapping is reestablished after each rebalancing.
    /// 
    ///  4. Consumer offset tracking:
    ///  /consumers/[group_id]/offsets/[topic]/[broker_id-partition_id] --> offset_counter_value
    ///  Each consumer tracks the offset of the latest message consumed for each partition.
    /// </summary>
    internal class ZookeeperConsumerConnector : IConsumerConnector
    {
        public static readonly FetchedDataChunk ShutdownCommand = new FetchedDataChunk(null, null, -1L);

        public ConsumerConfig Config { get; private set; }

        public bool EnableFetcher { get; private set; }

        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly AtomicBoolean isShuttingDown = new AtomicBoolean(false);

        private readonly object rebalanceLock = new object();

        private ConsumerFetcherManager fetcher;

        private ZkClient zkClient;

        private Pool<string, Pool<int, PartitionTopicInfo>> topicRegistry = new Pool<string, Pool<int, PartitionTopicInfo>>();

        private Pool<TopicAndPartition, long> checkpointedOffsets = new Pool<TopicAndPartition, long>();

        private readonly Pool<Tuple<string, string>, BlockingCollection<FetchedDataChunk>> topicThreadIdAndQueues = new Pool<Tuple<string, string>, BlockingCollection<FetchedDataChunk>>();

        private readonly KafkaScheduler scheduler = new KafkaScheduler();

        private readonly AtomicBoolean messageStreamCreated = new AtomicBoolean(false);

        private ZKSessionExpireListener sessionExpirationListener;

        private ZKTopicPartitionChangeListener topicPartitionChangeListener;

        private IZKRebalancerListener loadBalancerListener;

        private ZookeeperTopicEventWatcher wildcardTopicWatcher;

        public ZookeeperConsumerConnector(ConsumerConfig config, bool enableFetcher = true)
        {
            this.Config = config;
            this.EnableFetcher = enableFetcher;

            string consumerUuid;
            if (config.ConsumerId != null)
            {
                consumerUuid = config.ConsumerId;
            }
            else
            {
                // generate unique consumerId automatically
                var uuid = Guid.NewGuid();
                consumerUuid = string.Format(
                    "{0}-{1}-{2}", Dns.GetHostName(), DateTimeHelper.CurrentTimeMilis(), BitConverter.ToString(uuid.ToByteArray()).Substring(0, 8));
            }

            this.consumerIdString = config.GroupId + "_" + consumerUuid;

            this.logIdent = "[" + this.consumerIdString + "]";

            this.ConnectZk();
            this.CreateFetcher();

            if (config.AutoCommitEnable)
            {
                this.scheduler.Startup();
                Logger.InfoFormat("starting auto committer every {0} ms", config.AutoCommitIntervalMs);
                this.scheduler.Schedule("kafka-consumer-autocommit", this.AutoCommit, TimeSpan.FromMilliseconds(config.AutoCommitIntervalMs), TimeSpan.FromMilliseconds(config.AutoCommitIntervalMs));
            }

            KafkaMetricsReporter.StartReporters(this.Config);
        }

        private readonly string consumerIdString;

        private string logIdent;

        public IDictionary<string, IList<KafkaStream<byte[], byte[]>>> CreateMessageStreams(IDictionary<string, int> topicCountMap)
        {
            return this.CreateMessageStreams(topicCountMap, new DefaultDecoder(), new DefaultDecoder());
        }

        public IDictionary<string, IList<KafkaStream<TKey, TValue>>> CreateMessageStreams<TKey, TValue>(IDictionary<string, int> topicCountMap, IDecoder<TKey> keyDecoder, IDecoder<TValue> valueDecoder)
        {
            if (this.messageStreamCreated.GetAndSet(true))
            {
                throw new Exception(this.GetType().Name + " can create message streams at most once");
            }

            return this.Consume(topicCountMap, keyDecoder, valueDecoder);
        }

        public IList<KafkaStream<TKey, TValue>> CreateMessageStreamsByFilter<TKey, TValue>(
            TopicFilter topicFilter, int numStreams = 1, IDecoder<TKey> keyDecoder = null, IDecoder<TValue> valueDecoder = null)
        {
            var wildcardStreamsHandler = new WildcardStreamsHandler<TKey, TValue>(
                this, topicFilter, numStreams, keyDecoder, valueDecoder);

            return wildcardStreamsHandler.Streams();
        }

        private void CreateFetcher()
        {
            if (this.EnableFetcher)
            {
                this.fetcher = new ConsumerFetcherManager(consumerIdString, this.Config, zkClient);
            }
        }

        public void ConnectZk()
        {
            Logger.InfoFormat("Connecting to zookeeper instance at " + this.Config.ZooKeeper.ZkConnect);
            this.zkClient = new ZkClient(
                this.Config.ZooKeeper.ZkConnect,
                this.Config.ZooKeeper.ZkSessionTimeoutMs,
                this.Config.ZooKeeper.ZkConnectionTimeoutMs,
                new ZkStringSerializer());
        }

        public void Shutdown()
        {
            lock (rebalanceLock)
            {
                var canShutdown = isShuttingDown.CompareAndSet(false, true);
                if (canShutdown)
                {
                    Logger.Info("ZKConsumerConnector shutting down");
                    if (wildcardTopicWatcher != null)
                    {
                        wildcardTopicWatcher.Shutdown();
                    }

                    try
                    {
                        if (this.Config.AutoCommitEnable)
                        {
                            scheduler.Shutdown();
                        }

                        if (fetcher != null)
                        {
                            fetcher.StopConnections();
                        }

                        this.SendShutdownToAllQueues();
                        if (this.Config.AutoCommitEnable)
                        {
                            this.CommitOffsets();
                        }

                        if (zkClient != null)
                        {
                            zkClient.Dispose();
                            zkClient = null;
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.Fatal("error during consumer connector shutdown", e);
                    }

                    Logger.Info("ZKConsumerConnector shut down completed");
                }
            }
        }

        private IDictionary<string, IList<KafkaStream<TKey, TValue>>> Consume<TKey, TValue>(IDictionary<string, int> topicCountMap, IDecoder<TKey> keyDecoder, IDecoder<TValue> valueDecoder)
        {
            Logger.Debug("entering consume");
            if (topicCountMap == null)
            {
                throw new ArgumentNullException("topicCountMap");
            }

            var topicCount = TopicCount.ConstructTopicCount(consumerIdString, topicCountMap);

            var topicThreadIds = topicCount.GetConsumerThreadIdsPerTopic();

            // make a list of (queue,stream) pairs, one pair for each threadId
            var queuesAndStreams = topicThreadIds.Values.SelectMany(threadIdSet => threadIdSet.Select(_ =>
                {
                    var queue = new BlockingCollection<FetchedDataChunk>(this.Config.QueuedMaxMessages);
                    var stream = new KafkaStream<TKey, TValue>(
                        queue, this.Config.ConsumerTimeoutMs, keyDecoder, valueDecoder, this.Config.ClientId);
                    return Tuple.Create(queue, stream);
                })).ToList();

            var dirs = new ZKGroupDirs(this.Config.GroupId);
            this.RegisterConsumerInZK(dirs, consumerIdString, topicCount);
            ReinitializeConsumer(topicCount, queuesAndStreams);

            return (IDictionary<string, IList<KafkaStream<TKey, TValue>>>)loadBalancerListener.KafkaMessageAndMetadataStreams;
        }

        // this API is used by unit tests only
        public Pool<string, Pool<int, PartitionTopicInfo>> TopicRegistry
        {
            get
            {
                return this.topicRegistry;
            }
        }

        private void RegisterConsumerInZK(ZKGroupDirs dirs, string consumerIdString, TopicCount topicCount)
        {
            Logger.InfoFormat("begin registering consumer {0} in ZK", consumerIdString);
            var timestamp = DateTimeHelper.CurrentTimeMilis();
            var consumerRegistrationInfo = JsonConvert.SerializeObject(new
                                                                           {
                                                                               version = 1,
                                                                               subscription = topicCount.TopicCountMap,
                                                                               pattern = topicCount.Pattern,
                                                                               timestamp
                                                                           });

                ZkUtils.CreateEphemeralPathExpectConflictHandleZKBug(
                    zkClient,
                    dirs.ConsumerRegistryDir + "/" + consumerIdString,
                    consumerRegistrationInfo,
                    null,
                    (consumerZKstring, consumer) => true,
                    this.Config.ZooKeeper.ZkSessionTimeoutMs);

            Logger.InfoFormat("end registering consumer {0} in ZK", consumerIdString);
        }

        private void SendShutdownToAllQueues()
        {
            foreach (var queue in topicThreadIdAndQueues.Values)
            {
                Logger.DebugFormat("clearing up queue");
                queue.Clear();
                queue.TryAdd(ShutdownCommand);
                Logger.Debug("Cleared queue and sent shutdown command");
            }
        }

        public void AutoCommit()
        {
            Logger.Debug("auto committing");
            try
            {
                this.CommitOffsets();
            }
            catch (Exception e)
            {
                // log it and let it go
                Logger.Error("exception during autoCommit", e);
            }
        }

        public void CommitOffsets()
        {
            if (zkClient == null)
            {
                Logger.Error("zk client is null. Cannot commit offsets");
                return;
            }

            foreach (var topicAndInfo in TopicRegistry)
            {
                var topic = topicAndInfo.Key;
                var infos = topicAndInfo.Value;
                var topicDirs = new ZKGroupTopicDirs(this.Config.GroupId, topic);
                foreach (var info in infos.Values)
                {
                    var newOffset = info.GetConsumeOffset();
                    if (newOffset != checkpointedOffsets[new TopicAndPartition(topic, info.PartitionId)])
                    {
                        try
                        {
                            ZkUtils.UpdatePersistentPath(
                                zkClient, topicDirs.ConsumerOffsetDir + "/" + info.PartitionId, newOffset.ToString(CultureInfo.InvariantCulture));
                            checkpointedOffsets[new TopicAndPartition(topic, info.PartitionId)] = newOffset;
                        }
                        catch (Exception e)
                        {
                            // log it and let it go
                            Logger.Warn("Exception during commitOffsets", e);
                        }

                        Logger.DebugFormat("Committed offset {0} for topic {1}", newOffset, info);
                    }
                }
            }
        }

        internal class ZKSessionExpireListener : IZkStateListener
        {
            private readonly ZookeeperConsumerConnector parent;

            public ZKGroupDirs Dirs { get; private set; }

            public string ConsumerIdString { get; private set; }

            public TopicCount TopicCount { get; private set; }

            public IZKRebalancerListener LoadbalancerListener { get; private set; }

            public ZKSessionExpireListener(ZookeeperConsumerConnector parent, ZKGroupDirs dirs, string consumerIdString, TopicCount topicCount, IZKRebalancerListener loadbalancerListener)
            {
                this.parent = parent;
                this.Dirs = dirs;
                this.ConsumerIdString = consumerIdString;
                this.TopicCount = topicCount;
                this.LoadbalancerListener = loadbalancerListener;
            }

            public void HandleStateChanged(KeeperState state)
            {
                // do nothing, since zkclient will do reconnect for us.
            }

            /// <summary>
            /// Called after the zookeeper session has expired and a new session has been created. You would have to re-create
            /// any ephemeral nodes here.
            /// </summary>
            public void HandleNewSession()
            {
                /*
                 * When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has reestablished a
                 * connection for us. We need to release the ownership of the current consumer and re-register this
                 * consumer in the consumer registry and trigger a rebalance. */

                Logger.InfoFormat("ZK expired; release old broker parition ownership; re-register consumer {0}", ConsumerIdString);
                LoadbalancerListener.ResetState();
                parent.RegisterConsumerInZK(this.Dirs, ConsumerIdString, TopicCount);

                // explicitly trigger load balancing for this consumer
                LoadbalancerListener.SyncedRebalance();

                // There is no need to resubscribe to child and state changes.
                // The child change watchers will be set inside rebalance when we read the children list.
            }
        }

        internal class ZKTopicPartitionChangeListener : IZkDataListener
        {
            private ZookeeperConsumerConnector parent;

            public IZKRebalancerListener LoadbalancerListener { get; private set; }

            public ZKTopicPartitionChangeListener(
                ZookeeperConsumerConnector parent, IZKRebalancerListener loadBalancerListener)
            {
                this.parent = parent;
                this.LoadbalancerListener = loadBalancerListener;
            }

            public void HandleDataChange(string dataPath, object data)
            {
                try
                {
                    Logger.InfoFormat(
                        "Topic info for path {0} changed to {1}, triggering rebalance", dataPath, data);

                    // queue up the rebalance event
                    LoadbalancerListener.RebalanceEventTriggered();

                    // There is no need to re-subscribe the watcher since it will be automatically
                    // re-registered upon firing of this event by zkClient
                }
                catch (Exception e)
                {
                    Logger.Error("Error while handling topic partition change for Data path " + dataPath, e);
                }
            }

            public void HandleDataDeleted(string dataPath)
            {
                Logger.WarnFormat("Topic for path {0} gets deleted, which should not happen at this time", dataPath);
            }
        }

        internal interface IZKRebalancerListener : IZkChildListener
        {
            void RebalanceEventTriggered();

            void SyncedRebalance();

            void ResetState();

            object KafkaMessageAndMetadataStreams { get; }
        }

        internal class ZKRebalancerListener<TKey, TValue> : IZkChildListener, IZKRebalancerListener
        {
            private readonly ZookeeperConsumerConnector parent;

            private readonly string group;

            private readonly string consumerIdString;

            public object KafkaMessageAndMetadataStreams { get; private set; }

            private bool isWatcherTriggered;

            private readonly ReentrantLock @lock;

            private readonly ICondition cond;

            private readonly Thread watcherExecutorThread;

            public ZKRebalancerListener(
                ZookeeperConsumerConnector parent,
                string group,
                string consumerIdString,
                IDictionary<string, IList<KafkaStream<TKey, TValue>>> kafkaMessageAndMetadataStreams)
            {
                this.parent = parent;
                this.group = group;
                this.consumerIdString = consumerIdString;
                this.KafkaMessageAndMetadataStreams = kafkaMessageAndMetadataStreams;

                this.@lock = new ReentrantLock();
                this.cond = this.@lock.NewCondition();

                this.watcherExecutorThread = new Thread(() =>
                    {
                        Logger.InfoFormat("starting watcher executor thread for consumer {0}", consumerIdString);
                        bool doRebalance;
                        while (!parent.isShuttingDown.Get())
                        {
                            try
                            {
                                this.@lock.Lock();
                                try
                                {
                                    if (!isWatcherTriggered)
                                    {
                                        cond.Await(TimeSpan.FromMilliseconds(1000)); // wake up periodically so that it can check the shutdown flag
                                    }
                                }
                                finally
                                {
                                    doRebalance = isWatcherTriggered;
                                    isWatcherTriggered = false;
                                    @lock.Unlock();
                                }

                                if (doRebalance)
                                {
                                    this.SyncedRebalance();
                                }
                            }
                            catch (Exception e)
                            {
                                Logger.Error("Error during syncedRebalance", e);
                            }
                        }

                        Logger.InfoFormat("Stoppping watcher executer thread for consumer {0}", consumerIdString);
                    });
                this.watcherExecutorThread.Name = consumerIdString + "_watcher_executor";

                this.watcherExecutorThread.Start();
            }

            public void HandleChildChange(string parentPath, IEnumerable<string> curChilds)
            {
                this.RebalanceEventTriggered();
            }

            public void RebalanceEventTriggered()
            {
                this.@lock.Lock();
                try
                {
                    this.isWatcherTriggered = true;
                    cond.SignalAll();
                }
                finally
                {
                    this.@lock.Unlock();
                }
            }

            private void DeletePartitionOwnershipFromZK(string topic, int partition)
            {
                var topicDirs = new ZKGroupTopicDirs(group, topic);
                var znode = topicDirs.ConsumerOwnerDir + "/" + partition;
                ZkUtils.DeletePath(this.parent.zkClient, znode);
                Logger.DebugFormat("Consumer {0} releasing {1}", consumerIdString, znode);
            }

            private void ReleasePartitionOwnership(Pool<string, Pool<int, PartitionTopicInfo>> localTopicRegistry)
            {
                Logger.Info("Releasing partition ownership");
                foreach (var topicAndInfos in localTopicRegistry)
                {
                    var topic = topicAndInfos.Key;
                    var infos = topicAndInfos.Value;
                    foreach (var partition in infos.Keys)
                    {
                        this.DeletePartitionOwnershipFromZK(topic, partition);
                    }

                    Pool<int, PartitionTopicInfo> _;
                    localTopicRegistry.TryRemove(topic, out _);
                }
            }

            public void ResetState()
            {
                this.parent.topicRegistry.Clear();
            }

            public void SyncedRebalance()
            {
                lock (parent.rebalanceLock)
                {
                    if (parent.isShuttingDown.Get())
                    {
                        return;
                    }
                    else
                    {
                        for (var i = 1; i <= parent.Config.RebalanceMaxRetries; i++)
                        {
                            Logger.InfoFormat("begin rebalancing consumer {0} try #{1}", consumerIdString, i);
                            var done = false;
                            Cluster cluster = null;
                            try
                            {
                                cluster = ZkUtils.GetCluster(parent.zkClient);
                                done = this.Rebalance(cluster);
                            }
                            catch (Exception e)
                            {
                                /** occasionally, we may hit a ZK exception because the ZK state is changing while we are iterating.
                                 * For example, a ZK node can disappear between the time we get all children and the time we try to get
                                 * the value of a child. Just let this go since another rebalance will be triggered.
                                 **/
                                Logger.Info("Exception during rebalance", e);
                            }

                            Logger.InfoFormat("end rebalancing consumer {0} try #{1}", consumerIdString, i);
                            if (done)
                            {
                                return;
                            }
                            else
                            {
                                /* Here the cache is at a risk of being stale. To take future rebalancing decisions correctly, we should
                                * clear the cache */
                                Logger.Info("Rebalancing attempt failed. Clearing the cache before the next rebalancing operation is triggered");
                            }

                            // stop all fetchers and clear all the queues to avoid Data duplication
                            CloseFetchersForQueues(cluster, (IDictionary<string, IList<KafkaStream<TKey, TValue>>>)KafkaMessageAndMetadataStreams, parent.topicThreadIdAndQueues.Select(x => x.Value).ToList());
                            Thread.Sleep(parent.Config.RebalanceBackoffMs);
                        }
                    }
                }

                throw new ConsumerRebalanceFailedException(
                    consumerIdString + " can't rebalance after " + parent.Config.RebalanceMaxRetries + " retries");
            }

            private bool Rebalance(Cluster cluster)
            {
                var myTopicThreadIdsMap =
                    TopicCount.ConstructTopicCount(group, consumerIdString, parent.zkClient)
                               .GetConsumerThreadIdsPerTopic();
                var consumersPerTopicMap = ZkUtils.GetConsumersPerTopic(parent.zkClient, group);
                var brokers = ZkUtils.GetAllBrokersInCluster(parent.zkClient);
                if (brokers.Count == 0)
                {
                    // This can happen in a rare case when there are no brokers available in the cluster when the consumer is started.
                    // We log an warning and register for child changes on brokers/id so that rebalance can be triggered when the brokers
                    // are up.
                    Logger.Warn("no brokers found when trying to rebalance.");
                    parent.zkClient.SubscribeChildChanges(ZkUtils.BrokerIdsPath, parent.loadBalancerListener);
                    return true;
                }
                else
                {
                    var partitionsAssignmentPerTopicMap = ZkUtils.GetPartitionAssignmentForTopics(
                        parent.zkClient, myTopicThreadIdsMap.Keys.ToList());
                    var partitionsPerTopicMap = partitionsAssignmentPerTopicMap.ToDictionary(
                        p => p.Key, p => p.Value.Keys.OrderBy(x => x).ToList());

                     /**
                     * fetchers must be stopped to avoid Data duplication, since if the current
                     * rebalancing attempt fails, the partitions that are released could be owned by another consumer.
                     * But if we don't stop the fetchers first, this consumer would continue returning Data for released
                     * partitions in parallel. So, not stopping the fetchers leads to duplicate Data.
                     */

                    this.CloseFetchers(cluster, (IDictionary<string, IList<KafkaStream<TKey, TValue>>>) KafkaMessageAndMetadataStreams, myTopicThreadIdsMap);

                    this.ReleasePartitionOwnership(parent.topicRegistry);

                    var partitionOwnershipDecision = new Dictionary<Tuple<string, int>, string>();
                    var currentTopicRegistry = new Pool<string, Pool<int, PartitionTopicInfo>>();

                    foreach (var topicAndConsumerThreadIsSet in myTopicThreadIdsMap)
                    {
                        var topic = topicAndConsumerThreadIsSet.Key;
                        var consumerThreadIdSet = topicAndConsumerThreadIsSet.Value;

                        currentTopicRegistry[topic] = new Pool<int, PartitionTopicInfo>();

                        var topicDirs = new ZKGroupTopicDirs(group, topic);
                        var curConsumers = consumersPerTopicMap.Get(topic);
                        var curPartitions = partitionsPerTopicMap.Get(topic);

                        var nPartsPerConsumer = curPartitions.Count / curConsumers.Count;
                        var nConsumersWithExtraPart = curPartitions.Count % curConsumers.Count;

                        Logger.InfoFormat("Consumer {0} rebalancing the following partitions: {1} for topic {2} with consumers: {3}", consumerIdString, string.Join(",", curPartitions), topic, string.Join(",", curConsumers));

                        foreach (var consumerThreadId in consumerThreadIdSet)
                        {
                            var myConsumerPosition = curConsumers.IndexOf(consumerThreadId);
                            Contract.Assert(myConsumerPosition >= 0);
                            var startPart = (nPartsPerConsumer * myConsumerPosition)
                                            + Math.Min(nConsumersWithExtraPart, myConsumerPosition);
                            var nParts = nPartsPerConsumer + (myConsumerPosition + 1 > nConsumersWithExtraPart ? 0 : 1);

                            /**
                             *   Range-partition the sorted partitions to consumers for better locality.
                             *  The first few consumers pick up an extra partition, if any.
                             */

                            if (nParts <= 0)
                            {
                                Logger.WarnFormat(
                                    "No broker partitions consumed by consumer thread {0} for topic {1}",
                                    consumerThreadId,
                                    topic);
                            }
                            else
                            {
                                for (var i = startPart; i < startPart + nParts; i++)
                                {
                                    var partition = curPartitions[i];
                                    Logger.InfoFormat("{0} attempting to claim partition {1}", consumerThreadId, partition);
                                    this.AddPartitionTopicInfo(currentTopicRegistry, topicDirs, partition, topic, consumerThreadId);

                                    // record the partition ownership decision
                                    partitionOwnershipDecision[Tuple.Create(topic, partition)] = consumerThreadId;
                                }
                            }
                        }
                    }

                    /**
                     * move the partition ownership here, since that can be used to indicate a truly successful rebalancing attempt
                     * A rebalancing attempt is completed successfully only after the fetchers have been started correctly
                     */
                    if (this.ReflectPartitionOwnershipDecision(partitionOwnershipDecision))
                    {
                        Logger.Info("Updating the cache");
                        Logger.Debug("Partitions per topic cache " + JObject.FromObject(partitionsPerTopicMap).ToString(Formatting.None));
                        Logger.Debug("Consumers per topic cache " + JObject.FromObject(consumersPerTopicMap).ToString(Formatting.None));
                        parent.topicRegistry = currentTopicRegistry;
                        this.UpdateFetcher(cluster);
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            }

            private void CloseFetchersForQueues(
                Cluster cluster,
                IDictionary<string, IList<KafkaStream<TKey, TValue>>> messageStreams,
                IEnumerable<BlockingCollection<FetchedDataChunk>> queuesToBeCleared)
            {
                var allPartitionInfos = parent.topicRegistry.Values.SelectMany(p => p.Values).ToList();
                if (parent.fetcher != null)
                {
                    parent.fetcher.StopConnections();
                    ClearFetcherQueues(allPartitionInfos, cluster, queuesToBeCleared, messageStreams);
                    Logger.Info("Committing all offsets after clearing the fetcher queues");
                    /**
                      * here, we need to commit offsets before stopping the consumer from returning any more messages
                      * from the current Data chunk. Since partition ownership is not yet released, this commit offsets
                      * call will ensure that the offsets committed now will be used by the next consumer thread owning the partition
                      * for the current Data chunk. Since the fetchers are already shutdown and this is the last chunk to be iterated
                      * by the consumer, there will be no more messages returned by this iterator until the rebalancing finishes
                      * successfully and the fetchers restart to fetch more Data chunks
                      **/
                    if (parent.Config.AutoCommitEnable)
                    {
                        parent.CommitOffsets();
                    }
                }
            }

            private void ClearFetcherQueues(
                IEnumerable<PartitionTopicInfo> topicInfos,
                Cluster cluster,
                IEnumerable<BlockingCollection<FetchedDataChunk>> queuesToBeCleared,
                IDictionary<string, IList<KafkaStream<TKey, TValue>>> messageStreams)
            {
                // Clear all but the currently iterated upon chunk in the consumer thread's queue
                foreach (var queue in queuesToBeCleared)
                {
                    queue.Clear();
                }

                Logger.Info("Cleared all relevant queues for this fetcher");

                // Also clear the currently iterated upon chunk in the consumer threads
                if (messageStreams != null)
                {
                    foreach (var stream in messageStreams.Values)
                    {
                        foreach (var s2 in stream)
                        {
                            s2.Clear();
                        }
                    }
                }

                Logger.Info("Cleared the Data chunks in all the consumer message iterators");
            }

            private void CloseFetchers(
                Cluster cluster,
                IDictionary<string, IList<KafkaStream<TKey, TValue>>> messageStreams,
                IDictionary<string, ISet<string>> relevantTopicThreadIdsMap)
            {
                // only clear the fetcher queues for certain topic partitions that *might* no longer be served by this consumer
                // after this rebalancing attempt
                var queuesTobeCleared =
                    parent.topicThreadIdAndQueues.Where(q => relevantTopicThreadIdsMap.ContainsKey(q.Key.Item1))
                                          .Select(q => q.Value)
                                          .ToList();
                this.CloseFetchersForQueues(cluster, messageStreams, queuesTobeCleared);
            }

            private void UpdateFetcher(Cluster cluster)
            {
                // update partitions for fetcher
                var allPartitionInfos = new List<PartitionTopicInfo>();
                foreach (var partitionInfos in parent.topicRegistry.Values)
                {
                    allPartitionInfos.AddRange(partitionInfos.Values);
                }
                Logger.InfoFormat("Consumer {0} selected partitions: {1}", consumerIdString, string.Join(",", allPartitionInfos.OrderBy(x => x.PartitionId).Select(x => x.ToString())));

                if (parent.fetcher != null)
                {
                    parent.fetcher.StartConnections(allPartitionInfos, cluster);
                }
            }

            private bool ReflectPartitionOwnershipDecision(
                IDictionary<Tuple<string, int>, string> partitionOwnershipDecision)
            {
                var successfullyOwnedPartitions = new List<Tuple<string, int>>();
                var partitionOwnershipSuccessful = partitionOwnershipDecision.Select(
                    partitionOwner =>
                        {
                            var topic = partitionOwner.Key.Item1;
                            var partition = partitionOwner.Key.Item2;
                            var consumerThreadId = partitionOwner.Value;
                            var partitionOwnerPath = ZkUtils.GetConsumerPartitionOwnerPath(group, topic, partition);
                            try
                            {
                                ZkUtils.CreateEphemeralPathExpectConflict(
                                    parent.zkClient, partitionOwnerPath, consumerThreadId);
                                Logger.InfoFormat(
                                    "{0} successfully owner partition {1} for topic {2}",
                                    consumerThreadId,
                                    partition,
                                    topic);
                                successfullyOwnedPartitions.Add(Tuple.Create(topic, partition));
                                return true;
                            }
                            catch (ZkNodeExistsException)
                            {
                                // The node hasn't been deleted by the original owner. So wait a bit and retry.
                                Logger.InfoFormat("waiting for the partition ownership to be deleted:" + partition);
                                return false;
                            }
                        }).ToList();
                var hasPartitionOwnershipFailed = partitionOwnershipSuccessful.Aggregate(
                    0, (sum, decision) => (sum + (decision ? 0 : 1)));
                /* even if one of the partition ownership attempt has failed, return false */

                if (hasPartitionOwnershipFailed > 0)
                {
                    // remove all paths that we have owned in ZK
                    foreach (var topicAndPartition in successfullyOwnedPartitions)
                    {
                        this.DeletePartitionOwnershipFromZK(topicAndPartition.Item1, topicAndPartition.Item2);
                    }

                    return false;
                }

                return true;
            }

            private void AddPartitionTopicInfo(
                Pool<string, Pool<int, PartitionTopicInfo>> currentTopicRegistry,
                ZKGroupTopicDirs topicDirs,
                int partition,
                string topic,
                string consumerThreadId)
            {
                var partTopicInfoMap = currentTopicRegistry.Get(topic);

                var znode = topicDirs.ConsumerOffsetDir + "/" + partition;
                var offsetString = ZkUtils.ReadDataMaybeNull(parent.zkClient, znode).Item1;

                // If first time starting a consumer, set the initial offset to -1
                var offset = (offsetString != null) ? long.Parse(offsetString) : PartitionTopicInfo.InvalidOffset;

                var queue = parent.topicThreadIdAndQueues.Get(Tuple.Create(topic, consumerThreadId));
                var consumedOffset = new AtomicLong(offset);
                var fetchedOffset = new AtomicLong(offset);
                var partTopicInfo = new PartitionTopicInfo(
                    topic,
                    partition,
                    queue,
                    consumedOffset,
                    fetchedOffset,
                    new AtomicInteger(parent.Config.FetchMessageMaxBytes),
                    parent.Config.ClientId);

                partTopicInfoMap[partition] = partTopicInfo;
                Logger.DebugFormat("{0} selected new offset {1}", partTopicInfo, offset);
                parent.checkpointedOffsets[new TopicAndPartition(topic, partition)] = offset;
            }
        }

        private void ReinitializeConsumer<TKey, TValue>(
            TopicCount topicCount, IList<Tuple<BlockingCollection<FetchedDataChunk>, KafkaStream<TKey, TValue>>> queuesAndStreams)
        {
            var dirs = new ZKGroupDirs(this.Config.GroupId);

            // listener to consumer and partition changes
            if (loadBalancerListener == null)
            {
                var topicStreamsMaps = new Dictionary<string, IList<KafkaStream<TKey, TValue>>>();
                loadBalancerListener = new ZKRebalancerListener<TKey, TValue>(this, this.Config.GroupId, consumerIdString, topicStreamsMaps); 
            }

            // create listener for session expired event if not exist yet
            if (sessionExpirationListener == null)
            {
                sessionExpirationListener = new ZKSessionExpireListener(this, 
                    dirs, consumerIdString, topicCount, loadBalancerListener);
            }

            // create listener for topic partition change event if not exist yet
            if (topicPartitionChangeListener == null)
            {
                topicPartitionChangeListener = new ZKTopicPartitionChangeListener(this, loadBalancerListener);
            }

            var topicStreamsMap = (IDictionary<string, IList<KafkaStream<TKey, TValue>>>)loadBalancerListener.KafkaMessageAndMetadataStreams;

            // map of {topic -> Set(thread-1, thread-2, ...)}
            var consumerThreadIdsPerTopic = topicCount.GetConsumerThreadIdsPerTopic();

            IList<Tuple<BlockingCollection<FetchedDataChunk>, KafkaStream<TKey, TValue>>> allQueuesAndStreams = null;
            if (topicCount is WildcardTopicCount)
            {
                /*
                 * Wild-card consumption streams share the same queues, so we need to
                 * duplicate the list for the subsequent zip operation.
                 */
                 allQueuesAndStreams = Enumerable.Range(1, consumerThreadIdsPerTopic.Keys.Count).SelectMany(_ => queuesAndStreams).ToList();
            } 
            else if (topicCount is StaticTopicCount)
            {
                allQueuesAndStreams = queuesAndStreams;
            }

            var topicThreadIds = consumerThreadIdsPerTopic.SelectMany(topicAndThreadIds =>
                {
                    var topic = topicAndThreadIds.Key;
                    var threadIds = topicAndThreadIds.Value;
                    return threadIds.Select(id => Tuple.Create(topic, id));
                }).ToList();

            Contract.Assert(topicThreadIds.Count == allQueuesAndStreams.Count, string.Format("Mismatch betwen thread ID count ({0}) adn queue count ({1})", topicThreadIds.Count, allQueuesAndStreams.Count));

            var threadQueueStreamPairs = topicThreadIds.Zip(allQueuesAndStreams, Tuple.Create).ToList();

            foreach (var e in threadQueueStreamPairs)
            {
                var topicThreadId = e.Item1;
                var q = e.Item2.Item1;
                topicThreadIdAndQueues[topicThreadId] = q;
                Logger.DebugFormat("Adding topicThreadId {0} and queue {1} to topicThreadIdAndQueues Data structure", topicThreadId, string.Join(",", q));
                MetersFactory.NewGauge(this.Config.ClientId + "-" + this.Config.GroupId + "-" + topicThreadId.Item1 + "-" + topicThreadId.Item2 + "-FetchQueueSize", () => q.Count);
            }

            var groupedByTopic = threadQueueStreamPairs.GroupBy(x => x.Item1.Item1).ToList();

            foreach (var e in groupedByTopic)
            {
                var topic = e.Key;
                var streams = e.Select(x => x.Item2.Item2).ToList();
                topicStreamsMap[topic] = streams;
                Logger.DebugFormat("adding topic {0} and {1} stream to map", topic, streams.Count);
            }

            // listener to consumer and partition changes
            zkClient.SubscribeStateChanges(sessionExpirationListener);

            zkClient.SubscribeChildChanges(dirs.ConsumerRegistryDir, loadBalancerListener);

            foreach (var topicAndSteams in topicStreamsMap)
            {
                // register on broker partition path changes
                var topicPath = ZkUtils.BrokerTopicsPath + "/" + topicAndSteams.Key;
                zkClient.SubscribeDataChanges(topicPath, topicPartitionChangeListener);
            }

            // explicitly trigger load balancing for this consumer
            loadBalancerListener.SyncedRebalance();
        }
        

        internal class WildcardStreamsHandler<TKey, TValue> : ITopicEventHandler<string>
        {
            private TopicFilter topicFilter;

            private int numStreams;

            private IDecoder<TKey> keyDecoder;

            private IDecoder<TValue> valueDecoder;

            private readonly ZookeeperConsumerConnector parent;

            internal WildcardStreamsHandler(
                ZookeeperConsumerConnector parent,
                TopicFilter topicFilter,
                int numStreams,
                IDecoder<TKey> keyDecoder,
                IDecoder<TValue> valueDecoder)
            {
                this.parent = parent;
                this.topicFilter = topicFilter;
                this.numStreams = numStreams;
                this.keyDecoder = keyDecoder;
                this.valueDecoder = valueDecoder;

                if (parent.messageStreamCreated.GetAndSet(true))
                {
                    throw new Exception("Each consumer connector can create message streams by filter at most once.");
                }

                this.wildcardQueuesAndStreams = Enumerable.Range(1, numStreams).Select(e =>
                    {
                        var queue = new BlockingCollection<FetchedDataChunk>(this.parent.Config.QueuedMaxMessages);
                        var stream = new KafkaStream<TKey, TValue>(
                            queue,
                            this.parent.Config.ConsumerTimeoutMs,
                            keyDecoder,
                            valueDecoder,
                            this.parent.Config.ClientId);
                        return Tuple.Create(queue, stream);
                    }).ToList();

                this.wildcardTopics =
                    ZkUtils.GetChildrenParentMayNotExist(this.parent.zkClient, ZkUtils.BrokerTopicsPath)
                           .Where(topicFilter.IsTopicAllowed)
                           .ToList();

                this.wildcardTopicCount = TopicCount.ConstructTopicCount(
                    this.parent.consumerIdString, topicFilter, numStreams, this.parent.zkClient);

                this.dirs = new ZKGroupDirs(this.parent.Config.GroupId);

                this.parent.RegisterConsumerInZK(dirs, this.parent.consumerIdString, this.wildcardTopicCount);
                this.parent.ReinitializeConsumer(this.wildcardTopicCount, this.wildcardQueuesAndStreams);

                // Topic events will trigger subsequent synced rebalances.
                Logger.InfoFormat("Creating topic event watcher for topics {0}", topicFilter);
                this.parent.wildcardTopicWatcher = new ZookeeperTopicEventWatcher(this.parent.zkClient, this);

            }

            private List<Tuple<BlockingCollection<FetchedDataChunk>, KafkaStream<TKey, TValue>>> wildcardQueuesAndStreams;

            private List<string> wildcardTopics;

            private readonly WildcardTopicCount wildcardTopicCount;

            private readonly ZKGroupDirs dirs;

            public void HandleTopicEvent(IEnumerable<string> allTopics)
            {
                Logger.Debug("Handling topic event");
                var updatedTopics = allTopics.Where(topicFilter.IsTopicAllowed).ToList();

                var addedTopics = updatedTopics.Where(x => !wildcardTopics.Contains(x)).ToList();
                if (addedTopics.Any())
                {
                    Logger.InfoFormat("Topic event: added topics = {0}", string.Join(",", addedTopics));
                }

                /*
                   * Deleted topics are interesting (and will not be a concern until
                   * 0.8 release). We may need to remove these topics from the rebalance
                   * listener's map in reinitializeConsumer.
                   */
                var deletedTopics = wildcardTopics.Where(x => !updatedTopics.Contains(x)).ToList();
                if (deletedTopics.Any())
                {
                    Logger.InfoFormat("Topic event: deleted topics = {0}", string.Join(",", deletedTopics));
                }

                wildcardTopics = updatedTopics;
                Logger.InfoFormat("Topics to consume = {0}", string.Join(",", wildcardTopics));

                if (addedTopics.Any() || deletedTopics.Any())
                {
                    this.parent.ReinitializeConsumer(wildcardTopicCount, wildcardQueuesAndStreams);
                }
            }

            public List<KafkaStream<TKey, TValue>> Streams()
            {
                return wildcardQueuesAndStreams.Select(x => x.Item2).ToList();
            }
        }
    }
}