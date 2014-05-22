namespace Kafka.Tests.Utils
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using System.Threading;

    using Kafka.Client.Api;
    using Kafka.Client.Cfg;
    using Kafka.Client.Clusters;
    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Consumers;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Serializers;
    using Kafka.Client.Utils;
    using Kafka.Client.ZKClient;
    using Kafka.Tests.Custom.Server;

    using Spring.Threading.Locks;

    using Xunit;

    using log4net;

    //TODO: reorder methods
    internal static class TestUtils
    {
        static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private static readonly string IoTmpDir = Path.GetTempPath();

        private const string Letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

        private const string Digits = "0123456789";

        private const string LettersAndDigits = Letters + Digits;

        // A consistent random number generator to make tests repeatable
        private static Random seededRandom = new Random(192348092);

        private static Random random = new Random();

        private static readonly ISet<int> AvailablePorts = new HashSet<int>(Enumerable.Range(2100, 2200));

        public static void PortReleased(int port)
        {
            AvailablePorts.Add(port);
        }

        public static List<int> ChoosePorts(int count)
        {
            if (AvailablePorts.Count < count)
            {
                throw new KafkaException("Unable to get " + count + " ports as only " + AvailablePorts.Count + " left");
            }
            return Enumerable.Range(1, count).Select(idx =>
                {
                    var randomPort = AvailablePorts.ToArray()[random.Next(AvailablePorts.Count)];
                    AvailablePorts.Remove(randomPort);
                    return randomPort;
                }).ToList();
        }

        public static int ChoosePort()
        {
            return ChoosePorts(1).First();
        }

        public static DirectoryInfo TempDir()
        {
            return Directory.CreateDirectory(Path.Combine(IoTmpDir, "kafka-dir-" + random.Next(1000000)));
        }

        public static string TempFile()
        {
            while (true)
            {
                var path = Path.Combine(IoTmpDir, "kafka-" + random.Next(1000000));
                if (!File.Exists(path))
                {
                    File.WriteAllText(path, string.Empty);
                    return path;
                }
            }
        }

        public static List<TempKafkaConfig> CreateBrokerConfigs(int numConfigs, Func<int, Dictionary<string, string>> customProps = null)
        {
            return ChoosePorts(numConfigs).Select((port, node) => CreateBrokerConfig(node, port, customProps)).ToList();
        }

        public static List<BrokerConfiguration> GetBrokerListFromConfigs(List<TempKafkaConfig> configs)
        {
            return
                configs.Select(
                    c => new BrokerConfiguration() { BrokerId = c.BrokerId, Host = "localhost", Port = c.Port })
                       .ToList();
        }


        public static TempKafkaConfig CreateBrokerConfig(
            int nodeId, int port, Func<int, Dictionary<string, string>> customProps = null)
        {
            var props = new Dictionary<string, string>
                            {
                                { "broker.id", nodeId.ToString() },
                                { "host.name", "localhost" },
                                { "port", port.ToString() },
                                { "log.dir", TempDir().FullName.Replace("\\", "\\\\") },
                                { "zookeeper.connect", TestZkUtils.ZookeeperConnect },
                                { "replica.socket.timeout.ms", "1500" }
                            };
            if (customProps != null)
            {
                var overrides = customProps(nodeId);
                foreach (var kvp in overrides)
                {
                    props[kvp.Key] = kvp.Value;
                }
            }
            return TempKafkaConfig.Create(props);
        }

        public static ConsumerConfig CreateConsumerProperties(
            string zkConnect, string groupId, string consumerId, long consumerTimeout = -1)
        {
            var config = new ConsumerConfig();
            config.ZooKeeper = new ZkConfig();
            config.ZooKeeper.ZkConnect = zkConnect;
            config.GroupId = groupId;
            config.ConsumerId = consumerId;
            config.ConsumerTimeoutMs = (int)consumerTimeout;

            config.ZooKeeper.ZkSessionTimeoutMs = 400;
            config.ZooKeeper.ZkSyncTimeMs = 200;
            config.AutoCommitIntervalMs = 1000;
            config.RebalanceMaxRetries = 4;
            config.AutoOffsetReset = "smallest";
            config.NumConsumerFetchers = 2;
            return config;
        }

        public static SyncProducerConfig GetSyncProducerConfig(int port)
        {
            return new SyncProducerConfig
                       {
                           Host = "localhost",
                           Port = port,
                           RequestTimeoutMs = 500,
                           RequestRequiredAcks = 1,
                           KeySerializer = typeof(StringEncoder).AssemblyQualifiedName,
                           Serializer = typeof(StringEncoder).AssemblyQualifiedName
                       };
        }

        public static void UpdateConsumerOffset(ConsumerConfig config, string path, long offset)
        {
            var zkClient = new ZkClient(
                config.ZooKeeper.ZkConnect,
                config.ZooKeeper.ZkSessionTimeoutMs,
                config.ZooKeeper.ZkConnectionTimeoutMs,
                new ZkStringSerializer());
            ZkUtils.UpdatePersistentPath(zkClient, path, offset.ToString());
        }


        class MessageIterator : IteratorTemplate<Message>
        {
            private IIterator<MessageAndOffset> iter;

            public MessageIterator(IIterator<MessageAndOffset> iter)
            {
                this.iter = iter;
            }

            protected override Message MakeNext()
            {
                if (this.iter.HasNext())
                {
                    return iter.Next().Message;
                }
                return this.AllDone();
            }
        }

         public static IIterator<Message> GetMessageIterator(IIterator<MessageAndOffset> iter)
         {
             return new MessageIterator(iter);
         }

        public static List<string> GetMsgStrings(int n)
        {
            var buffer = new List<string>();
            for (var i = 0; i < n; i++)
            {
                buffer.Add("msg" + i);    
            }
            return buffer;
        }

        public static ProducerRequest ProduceRequest(
            string topic,
            int partition,
            ByteBufferMessageSet message,
            int acks = SyncProducerConfig.DefaultRequiredAcks,
            int timeout = SyncProducerConfig.DefaultAckTimeout,
            int correlationId = 0,
            string clientId = SyncProducerConfig.DefaultClientId)
        {
            return ProduceRequestWithAcks(
                new List<string> { topic }, new List<int> { partition }, message, acks, timeout, correlationId, clientId);
        }

        public static ProducerRequest ProduceRequestWithAcks(
            List<string> topics,
            List<int> partitions, ByteBufferMessageSet message, 
            int acks = SyncProducerConfig.DefaultRequiredAcks,
            int timeout = SyncProducerConfig.DefaultAckTimeout,
            int correlationId = 0,
            string clientId = SyncProducerConfig.DefaultClientId)
        {
            var data =
                topics.SelectMany(
                    topic =>
                    partitions.Select(partition => Tuple.Create(new TopicAndPartition(topic, partition), message)))
                      .ToList();

            return new ProducerRequest(
                correlationId, clientId, (short)acks, timeout, data.ToDictionary(x => x.Item1, x => x.Item2));
        }
    


        public static string RandomString(int len)
        {
            var b = new StringBuilder();
            for (int i = 0; i < len; i++)
            {
                b.Append(LettersAndDigits.ToCharArray()[seededRandom.Next(LettersAndDigits.Length)]);
            }
            return b.ToString();
        }

        /// <summary>
        /// Check that the buffer content from buffer.position() to buffer.limit() is equal
        /// </summary>
        /// <param name="b1"></param>
        /// <param name="b2"></param>
        public static void CheckEquals(ByteBuffer b1, ByteBuffer b2)
        {
            Assert.Equal(b1.Limit() - b1.Position, b2.Limit() - b2.Position);
            for (var i = 0; i < b1.Limit() - b1.Position; i++)
            {
                Assert.Equal(b1.Get((int)b1.Position + i), b2.Get((int)b1.Position + i));
            }
        }


        /// <summary>
         /// Throw an exception if the two iterators are of differing lengths or contain
         /// different messages on their Nth element
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="expected"></param>
        /// <param name="actual"></param>
        public static void CheckEquals<T>(IEnumerator<T> expected, IEnumerator<T> actual)
        {
            var length = 0;
            while (expected.MoveNext() && actual.MoveNext())
            {
                length++;
                Assert.Equal(expected.Current, actual.Current);
            }

            // check if the expected iterator is longer
            if (expected.MoveNext())
            {
                var length1 = length;
                while (expected.MoveNext())
                {
                    var current = expected.Current;
                    length1++;
                }
                Assert.False(false, "Iterators have uneven length -- first has more " + length1 + " > " + length);
            }

            // check if the actual iterator was longer
            if (actual.MoveNext())
            {
                var length2 = length;
                while (actual.MoveNext())
                {
                    var current = actual.Current;
                    length2++;
                }
                Assert.False(false, "Iterators have uneven length -- second has more " + length2 + " > " + length);
            }
        }

        /// <summary>
        /// Throw an exception if the two iterators are of differing lengths or contain
        /// different messages on their Nth element
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="expected"></param>
        /// <param name="actual"></param>
        public static void CheckEquals<T>(IEnumerable<T> expected, IEnumerable<T> actual)
        {
            CheckEquals(expected.GetEnumerator(), actual.GetEnumerator());
        }

        public static Producer<string, byte[]> CreateProducer(
            List<BrokerConfiguration> brokerList)
        {
            return CreateProducer(brokerList, new DefaultEncoder(), new StringEncoder());
        }

        public static Producer<TKey, TValue> CreateProducer<TKey, TValue>(
            List<BrokerConfiguration> brokerList, IEncoder<TValue> encoder, IEncoder<TKey> keyEncoder)
        {
            var config = new ProducerConfig();
            config.Brokers = brokerList;
            config.SendBufferBytes = 65536;
            config.RequestTimeoutMs = 10000;
            config.RetryBackoffMs = 10000;
            config.Serializer = encoder.GetType().AssemblyQualifiedName;
            config.KeySerializer = keyEncoder.GetType().AssemblyQualifiedName;
            return new Producer<TKey, TValue>(config);
        }

        public static ProducerConfig GetProducerConfig(List<BrokerConfiguration> brokerList, string partitioner)
        {
            ProducerConfig config = new ProducerConfig();
            config.Brokers = brokerList;
            config.PartitionerClass = partitioner;
            config.MessageSendMaxRetries = 3;
            config.RetryBackoffMs = 1000;
            config.RequestTimeoutMs = 500;
            config.RequestRequiredAcks = -1;
            config.Serializer = typeof(StringEncoder).AssemblyQualifiedName;
            config.KeySerializer = typeof(StringEncoder).AssemblyQualifiedName;
            return config;
        }

        public static int? WaitUntilLeaderIsElectedOrChanged(
            ZkClient zkClient, string topic, int partition, long timeoutMs, int? oldLeaderOpt = null)
        {
            var leaderLock = new ReentrantLock();
            var leaderExistsOrChanged = leaderLock.NewCondition();

            if (oldLeaderOpt.HasValue == false)
            {
                Logger.InfoFormat("Waiting for leader to be elected for partition [{0},{1}]", topic, partition);
            }
            else
            {
                Logger.InfoFormat("Waiting for leader for partition [{0},{1}] to be changed from old leader {2}", topic, partition, oldLeaderOpt.Value);
            }
            leaderLock.Lock();
            try
            {
                zkClient.SubscribeDataChanges(ZkUtils.GetTopicPartitionLeaderAndIsrPath(topic, partition), new LeaderExistsOrChangedListener(topic, partition, leaderLock, leaderExistsOrChanged, oldLeaderOpt, zkClient));
                leaderExistsOrChanged.Await(TimeSpan.FromMilliseconds(timeoutMs));

                // check if leader is elected
                var leader = ZkUtils.GetLeaderForPartition(zkClient, topic, partition);
                if (leader != null)
                {
                    if (oldLeaderOpt.HasValue == false)
                    {
                        Logger.InfoFormat("Leader {0} is elected for partition [{1},{2}]", leader, topic, partition);
                    }
                    else
                    {
                        Logger.InfoFormat(
                            "Leader for partition [{0},{1}] is changed from {2} to {3}",
                            topic,
                            partition,
                            oldLeaderOpt.Value,
                            leader);
                    }
                }
                else
                {
                    Logger.ErrorFormat("Timing out after {0} ms since leader is not elected for partition [{1},{2}]", timeoutMs, topic, partition);
                }
                return leader;
            }
            finally
            {
                leaderLock.Unlock();
            }
        }

        public static void WaitUntilMetadataIsPropagated(List<Process> serves, string topic, int partition, long timeout)
        {
            Thread.Sleep((int)timeout);
            //TODO 
        }

    }

    internal static class TestZkUtils
    {
        public static string ZookeeperConnect { get; private set; }

        public static int ZookeeperPort { get; private set; }

        static TestZkUtils()
        {
            ResetPorts();
        }

        public static void ResetPorts()
        {
            ZookeeperPort = TestUtils.ChoosePort();
            ZookeeperConnect = "127.0.0.1:" + ZookeeperPort;
        }

    }

    public class IntEncoder : IEncoder<int>
    {

         public IntEncoder(ProducerConfig config = null)
         {
         }

        public byte[] ToBytes(int t)
        {
            return Encoding.UTF8.GetBytes(t.ToString());
        }
    }

    public class StaticPartitioner : IPartitioner
    {
        public StaticPartitioner(ProducerConfig config)
        {
        }

        public int Partition(object data, int numPartitions)
        {
            return ((string)data).Length % numPartitions;
        }
    }

    public class FixedValuePartitioner : IPartitioner
    {
        public FixedValuePartitioner(ProducerConfig config)
        {
        }

        public int Partition(object data, int numPartitions)
        {
            return (int)data;
        }
    }
}