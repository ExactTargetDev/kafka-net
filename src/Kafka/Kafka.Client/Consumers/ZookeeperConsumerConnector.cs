namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Net;
    using System.Reflection;

    using Kafka.Client.Cfg;
    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Serializers;
    using Kafka.Client.Utils;
    using Kafka.Client.ZKClient;

    using ZooKeeperNet;

    using log4net;

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

        public ConsumerConfiguration config { get; private set; }

        public bool EnableFetcher { get; private set; }

        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly AtomicBoolean isShuttingDown = new AtomicBoolean(false);

        private readonly object rebalanceLock = new object();

        private ConsumerFetcherManager fetcher;

        private ZkClient zkClient;

        private Pool<string, Pool<int, PartitionTopicInfo>> topicRegistry = new Pool<string, Pool<int, PartitionTopicInfo>>();

        private Pool<TopicAndPartition, long> checkpointedOffsets = new Pool<TopicAndPartition, long>();

        private Pool<Tuple<string, string>, BlockingCollection<FetchedDataChunk>> topicThreadIdAndQueues = new Pool<Tuple<string, string>, BlockingCollection<FetchedDataChunk>>();

        private KafkaScheduler scheduler = new KafkaScheduler();

        private AtomicBoolean messageStreamCreated = new AtomicBoolean(false);

        private ZKSessionExpireListener sessionExpirationListener;

        private ZKTopicPartitionChangeListener topicPartitionChangeListener;

        private ZKRebalancerListener loadBalancerListener;

        private ZookeeperTopicEventWatcher wildcardTopicWatcher;

        public ZookeeperConsumerConnector(ConsumerConfiguration config, bool enableFetcher = true)
        {
            this.config = config;
            this.EnableFetcher = enableFetcher;

            string consumerUuid = null;
            if (config.ConsumerId != null)
            {
                consumerUuid = config.ConsumerId;
            }
            else
            {
                // generate unique consumerId automatically
                var uuid = Guid.NewGuid();
                consumerUuid = string.Format(
                    "{0}-{1}-{2}", Dns.GetHostName(), DateTime.Now, uuid.ToString().Substring(0, 8));
            }

            this.consumerIdString = config.GroupId + "_" + consumerUuid;

            this.logIdent = "[" + this.consumerIdString + "]";

            this.ConnectZk();
            this.CreateFetcher();

            if (config.AutoCommit)
            {
                this.scheduler.Startup();
                Logger.InfoFormat("starting auto committer every {0} ms", config.AutoCommitInterval);
                this.scheduler.Schedule("kafka-consumer-autocommit", this.AutoCommit, TimeSpan.FromMilliseconds(config.AutoCommitInterval), TimeSpan.FromMilliseconds(config.AutoCommitInterval));
            }

            // TODO: KafkaMetricsReporter.startReporters(config.props)

        }

        private string consumerIdString;

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
            return this.Consume<TKey, TValue>(topicCountMap, keyDecoder, valueDecoder);
        }

        public IList<KafkaStream<TKey, TValue>> CreateMessageStreamsByFilter<TKey, TValue>(
            TopicFilter topicFilter, int numStreams = 1, IDecoder<TKey> keyDecoder = null, IDecoder<TValue> valueDecoder = null)
        {
            throw new System.NotImplementedException();
        }

        private void CreateFetcher()
        {
            throw new NotImplementedException();    
        }

        public void ConnectZk()
        {
            Logger.InfoFormat("Connecting to zookeeper instance at " + config.ZooKeeper.ZkConnect);
            this.zkClient = new ZkClient(
                this.config.ZooKeeper.ZkConnect,
                this.config.ZooKeeper.ZkSessionTimeoutMs,
                this.config.ZooKeeper.ZkConnectionTimeoutMs,
                new ZkStringSerializer());
        }

        public void Shutdown()
        {
            throw new System.NotImplementedException();
        }

        private IDictionary<string, IList<KafkaStream<TKey, TValue>>> Consume<TKey, TValue>(IDictionary<string, int> topicCountMap, IDecoder<TKey> keyDecoder, IDecoder<TValue> valueDecoder)
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }

        private void SendShutdownToAllQueues()
        {
            throw new NotImplementedException();
        }

        public void AutoCommit()
        {
            throw new NotImplementedException();
        }

        public void CommitOffsets()
        {
            throw new System.NotImplementedException();
        }


        internal class ZKSessionExpireListener //TODO: : IZooKeeperChildListener
        {
            private ZookeeperConsumerConnector parent;

            public ZKGroupDirs dirs { get; private set; }

            public string ConsumerIdString { get; private set; }

            public TopicCount TopicCount { get; private set; }

            public ZKRebalancerListener LoadbalancerListener { get; private set; }

            public ZKSessionExpireListener(ZookeeperConsumerConnector parent, ZKGroupDirs dirs, string consumerIdString, TopicCount topicCount, ZKRebalancerListener loadbalancerListener)
            {
                this.parent = parent;
                this.dirs = dirs;
                this.ConsumerIdString = consumerIdString;
                this.TopicCount = topicCount;
                this.LoadbalancerListener = loadbalancerListener;
            }

            public void HandleStateChanged(KeeperState state)
            {
                // do nothing, since zkclient will do reconnect for us.
            }

            public void HandleNewSession()
            {
                throw new NotImplementedException();
            }
        }

        internal class ZKTopicPartitionChangeListener
        {
            //TODO:
        }

        internal class ZKRebalancerListener
        {
            //TODO:
        }

        //TODO: private def reinitializeConsumer[K,V](

    }
}