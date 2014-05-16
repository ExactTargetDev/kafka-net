namespace Kafka.Tests.Consumers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Threading;

    using Kafka.Client.Admin;
    using Kafka.Client.Consumers;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Serializers;
    using Kafka.Client.Utils;
    using Kafka.Client.ZKClient;
    using Kafka.Tests.Custom.Server;
    using Kafka.Tests.Integration;
    using Kafka.Tests.Utils;

    using Xunit;

    using log4net;
    using log4net.Core;

    public class ZookeeperConsumerConnectorTest : KafkaServerTestHarness
    {

        protected static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private const int RebalanceBackoutMs = 5000;

        private ZKGroupTopicDirs dirs;

        private string zookeeperConnect = TestZkUtils.ZookeeperConnect;

        private const int NumNodes = 2;

        private const int NumParts = 2;

        private const string Topic = "topic1";

        private const string Group = "group1";

        private const string Consumer0 = "consumer0";

        private const string Consumer1 = "consumer1";

        private const string Consumer2 = "consumer2";

        private const string Consumer3 = "consumer3";

        private static int nMessages = 2;

        public ZookeeperConsumerConnectorTest()
        {
            this.dirs = new ZKGroupTopicDirs(Group, Topic);
        }

        protected override List<TempKafkaConfig> CreateConfigs()
        {
            return TestUtils.CreateBrokerConfigs(
               NumNodes,
               idx => new Dictionary<string, string>
                           {
                               { "zookeeper.connect", "localhost:" + TestZkUtils.ZookeeperPort },
                               { "num.partitions", NumParts.ToString()}
                           });
        }

        [Fact]
        public void TestBasic()
        {
            //TODO: move to separate class and config statically
            log4net.Config.BasicConfigurator.Configure(
              new log4net.Appender.ConsoleAppender { Layout = new log4net.Layout.PatternLayout("%timestamp [%thread] %-5level %logger{2} %ndc - %message%newline"), Threshold = Level.Info }
            );
            
            // test consumer timeout logic
            var consumerConfig0 = TestUtils.CreateConsumerProperties(ZkConnect, Group, Consumer0, 200);
            var zkConsumerConnector0 = new ZookeeperConsumerConnector(consumerConfig0, true);
            var topicMessageSterams0 =
                zkConsumerConnector0.CreateMessageStreams(
                    new Dictionary<string, int> { { Topic, 1 } }, new StringDecoder(), new StringDecoder());

            // no messages to consume, we should hit timeout;
            // also the iterator should support re-entrant, so loop it twice
            for (var i = 0; i < 2; i++)
            {
                Assert.Throws<ConsumerTimeoutException>(
                    () => GetMessages(nMessages * 2, topicMessageSterams0));
            }

            zkConsumerConnector0.Shutdown();
            

            // send some messages to each broker
            var sentMessages1 =
                this.SendMessagesToBrokerPartition(Configs.First(), Topic, 0, nMessages)
                .Union(this.SendMessagesToBrokerPartition(Configs.Last(), Topic, 0, nMessages)).ToList();

            // wait to make sure the topic and partition have a leader for the successful case
            TestUtils.WaitUntilLeaderIsElectedOrChanged(this.ZkClient, Topic, 0, 500); 
            TestUtils.WaitUntilLeaderIsElectedOrChanged(this.ZkClient, Topic, 1, 500);

            TestUtils.WaitUntilMetadataIsPropagated(this.Servers, Topic, 0, 1000);
            TestUtils.WaitUntilMetadataIsPropagated(this.Servers, Topic, 1, 1000);

            // create a consuemr
            var consumerConfig1 = TestUtils.CreateConsumerProperties(ZkConnect, Group, Consumer1);
            var zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true);
            var topicMessageStreams1 =
                zkConsumerConnector1.CreateMessageStreams(
                    new Dictionary<string, int> { { Topic, 1 } }, new StringDecoder(), new StringDecoder());

            var receivedMessages1 = this.GetMessages(nMessages * 2, topicMessageStreams1);
            Assert.Equal(sentMessages1.OrderBy(x => x).ToArray(), receivedMessages1.OrderBy(x => x).ToArray());

            // also check partition ownership
            var actual_1 = this.GetZKChildrenValues(dirs.ConsumerOwnerDir);
            var expected_1 = new List<Tuple<string, string>>
                                 {
                                     Tuple.Create("0", "group1_consumer1-0"),
                                     Tuple.Create("1", "group1_consumer1-0")
                                 };
            Assert.Equal(expected_1, actual_1);

            // commit consumer offsets
            zkConsumerConnector1.CommitOffsets();

            // create a consumer
            var consumerConfig2 = TestUtils.CreateConsumerProperties(ZkConnect, Group, Consumer2);
            consumerConfig2.RebalanceBackoffMs = RebalanceBackoutMs;

            var zkConsumerConnector2 = new ZookeeperConsumerConnector(consumerConfig2, true);
            var topicMessageStreams2 =
                zkConsumerConnector2.CreateMessageStreams(
                    new Dictionary<string, int> { { Topic, 1 } }, new StringDecoder(), new StringDecoder());

            // send some messages to each broker
            var sentMessages2 =
                SendMessagesToBrokerPartition(Configs.First(), Topic, 0, nMessages)
                .Union(SendMessagesToBrokerPartition(Configs.Last(), Topic, 1, nMessages)).ToList();

            // wait to make sure the topic and partition have a leader for the successful case
            TestUtils.WaitUntilLeaderIsElectedOrChanged(ZkClient, Topic, 0, 500);
            TestUtils.WaitUntilLeaderIsElectedOrChanged(ZkClient, Topic, 1, 500);

            var receivedMessages2 =
                this.GetMessages(nMessages, topicMessageStreams1)
                    .Union(this.GetMessages(nMessages, topicMessageStreams2))
                    .ToList();
            Assert.Equal(sentMessages2.OrderBy(x => x).ToList(), receivedMessages2.OrderBy(x => x).ToList());

            // also check partition ownership
            var actual_2 = this.GetZKChildrenValues(dirs.ConsumerOwnerDir);
            var expected_2 = new List<Tuple<string, string>>
                                 {
                                     Tuple.Create("0", "group1_consumer1-0"),
                                     Tuple.Create("1", "group1_consumer2-0")
                                 };
            Assert.Equal(expected_2, actual_2);

            // create a consumer with empty map
            var consumerConfig3 = TestUtils.CreateConsumerProperties(ZkConnect, Group, Consumer3);
            var zkConsumerConnector3 = new ZookeeperConsumerConnector(consumerConfig3, true);
            var topicMessageStreams3 = zkConsumerConnector3.CreateMessageStreams(new Dictionary<string, int>());

            // send some messages to each broker
            var sentMessages3 =
                this.SendMessagesToBrokerPartition(Configs.First(), Topic, 0, nMessages)
                    .Union(this.SendMessagesToBrokerPartition(Configs.Last(), Topic, 1, nMessages))
                    .ToList();

            // wait to make sure the topic and partition have a leader for the successful case
            TestUtils.WaitUntilLeaderIsElectedOrChanged(ZkClient, Topic, 0, 500);
            TestUtils.WaitUntilLeaderIsElectedOrChanged(ZkClient, Topic, 1, 500);

            var receivedMessages3 =
                this.GetMessages(nMessages, topicMessageStreams1)
                    .Union(this.GetMessages(nMessages, topicMessageStreams2))
                    .ToList();
            Assert.Equal(sentMessages3.OrderBy(x => x).ToList(), receivedMessages3.OrderBy(x => x).ToList());

            // also check partition ownership
            var actual_3 = this.GetZKChildrenValues(dirs.ConsumerOwnerDir);
            Assert.Equal(expected_2, actual_3);

            zkConsumerConnector1.Shutdown();
            zkConsumerConnector2.Shutdown();
            zkConsumerConnector3.Shutdown();

            Logger.Info("all consumer connectors stopped");
        }
        
        [Fact]
        public void TestCompression()
        {
            // send some messages to each broker
            var sentMessages1 = this.SendMessagesToBrokerPartition(
                Configs.First(), Topic, 0, nMessages, CompressionCodecs.GZIPCompressionCodec)
                .Union(
                    this.SendMessagesToBrokerPartition(
                        Configs.First(), Topic, 1, nMessages, CompressionCodecs.GZIPCompressionCodec))
                .ToList();

            TestUtils.WaitUntilLeaderIsElectedOrChanged(this.ZkClient, Topic, 0, 500);
            TestUtils.WaitUntilLeaderIsElectedOrChanged(this.ZkClient, Topic, 1, 500);

            TestUtils.WaitUntilMetadataIsPropagated(this.Servers, Topic, 0, 1000);
            TestUtils.WaitUntilMetadataIsPropagated(this.Servers, Topic, 1, 1000);

            // create a consuemr
            var consumerConfig1 = TestUtils.CreateConsumerProperties(ZkConnect, Group, Consumer1);
            var zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true);
            var topicMessageStreams1 =
                zkConsumerConnector1.CreateMessageStreams(
                    new Dictionary<string, int> { { Topic, 1 } }, new StringDecoder(), new StringDecoder());

            var receivedMessages1 = this.GetMessages(nMessages * 2, topicMessageStreams1);
            Assert.Equal(sentMessages1.OrderBy(x => x).ToArray(), receivedMessages1.OrderBy(x => x).ToArray());

            // also check partition ownership
            var actual_1 = this.GetZKChildrenValues(dirs.ConsumerOwnerDir);
            var expected_1 = new List<Tuple<string, string>>
                                 {
                                     Tuple.Create("0", "group1_consumer1-0"),
                                     Tuple.Create("1", "group1_consumer1-0")
                                 };
            Assert.Equal(expected_1, actual_1);

            // commit consumer offsets
            zkConsumerConnector1.CommitOffsets();

            // create a consumer
            var consumerConfig2 = TestUtils.CreateConsumerProperties(ZkConnect, Group, Consumer2);
            consumerConfig2.RebalanceBackoffMs = RebalanceBackoutMs;

            var zkConsumerConnector2 = new ZookeeperConsumerConnector(consumerConfig2, true);
            var topicMessageStreams2 =
                zkConsumerConnector2.CreateMessageStreams(
                    new Dictionary<string, int> { { Topic, 1 } }, new StringDecoder(), new StringDecoder());

            // send some messages to each broker
            var sentMessages2 =
                SendMessagesToBrokerPartition(Configs.First(), Topic, 0, nMessages, CompressionCodecs.GZIPCompressionCodec)
                .Union(SendMessagesToBrokerPartition(Configs.Last(), Topic, 1, nMessages, CompressionCodecs.GZIPCompressionCodec)).ToList();

            // wait to make sure the topic and partition have a leader for the successful case
            TestUtils.WaitUntilLeaderIsElectedOrChanged(ZkClient, Topic, 0, 500);
            TestUtils.WaitUntilLeaderIsElectedOrChanged(ZkClient, Topic, 1, 500);

            var receivedMessages2 =
                this.GetMessages(nMessages, topicMessageStreams1)
                    .Union(this.GetMessages(nMessages, topicMessageStreams2))
                    .ToList();
            Assert.Equal(sentMessages2.OrderBy(x => x).ToList(), receivedMessages2.OrderBy(x => x).ToList());

            // also check partition ownership
            var actual_2 = this.GetZKChildrenValues(dirs.ConsumerOwnerDir);
            var expected_2 = new List<Tuple<string, string>>
                                 {
                                     Tuple.Create("0", "group1_consumer1-0"),
                                     Tuple.Create("1", "group1_consumer2-0")
                                 };
            Assert.Equal(expected_2, actual_2);

            // create a consumer with empty map
            var consumerConfig3 = TestUtils.CreateConsumerProperties(ZkConnect, Group, Consumer3);
            var zkConsumerConnector3 = new ZookeeperConsumerConnector(consumerConfig3, true);
            var topicMessageStreams3 = zkConsumerConnector3.CreateMessageStreams(new Dictionary<string, int>());

            // send some messages to each broker
            var sentMessages3 =
                this.SendMessagesToBrokerPartition(Configs.First(), Topic, 0, nMessages, CompressionCodecs.GZIPCompressionCodec)
                    .Union(this.SendMessagesToBrokerPartition(Configs.Last(), Topic, 1, nMessages, CompressionCodecs.GZIPCompressionCodec))
                    .ToList();

            // wait to make sure the topic and partition have a leader for the successful case
            TestUtils.WaitUntilLeaderIsElectedOrChanged(ZkClient, Topic, 0, 500);
            TestUtils.WaitUntilLeaderIsElectedOrChanged(ZkClient, Topic, 1, 500);

            var receivedMessages3 =
                this.GetMessages(nMessages, topicMessageStreams1)
                    .Union(this.GetMessages(nMessages, topicMessageStreams2))
                    .ToList();
            Assert.Equal(sentMessages3.OrderBy(x => x).ToList(), receivedMessages3.OrderBy(x => x).ToList());

            // also check partition ownership
            var actual_3 = this.GetZKChildrenValues(dirs.ConsumerOwnerDir);
            Assert.Equal(expected_2, actual_3);

            zkConsumerConnector1.Shutdown();
            zkConsumerConnector2.Shutdown();
            zkConsumerConnector3.Shutdown();

            Logger.Info("all consumer connectors stopped");
        }

        [Fact]
        public void TestCompressionSetConsumption()
        {
            // send some messages to each broker
            var sentMessages = this.SendMessagesToBrokerPartition(
                Configs.First(), Topic, 0, 200, CompressionCodecs.DefaultCompressionCodec)
                .Union(
                    this.SendMessagesToBrokerPartition(
                        Configs.First(), Topic, 1, 200, CompressionCodecs.DefaultCompressionCodec))
                .ToList();

            TestUtils.WaitUntilMetadataIsPropagated(this.Servers, Topic, 0, 1000);
            TestUtils.WaitUntilMetadataIsPropagated(this.Servers, Topic, 1, 1000);

            // create a consuemr
            var consumerConfig1 = TestUtils.CreateConsumerProperties(ZkConnect, Group, Consumer0);
            var zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true);
            var topicMessageStreams1 =
                zkConsumerConnector1.CreateMessageStreams(
                    new Dictionary<string, int> { { Topic, 1 } }, new StringDecoder(), new StringDecoder());

            var receivedMessages1 = this.GetMessages(400, topicMessageStreams1);
            Assert.Equal(sentMessages.OrderBy(x => x).ToArray(), receivedMessages1.OrderBy(x => x).ToArray());

            // also check partition ownership
            var actual_2 = this.GetZKChildrenValues(dirs.ConsumerOwnerDir);
            var expected_2 = new List<Tuple<string, string>>
                                 {
                                     Tuple.Create("0", "group1_consumer0-0"),
                                     Tuple.Create("1", "group1_consumer0-0")
                                 };
            Assert.Equal(expected_2, actual_2);
            zkConsumerConnector1.Shutdown();
        }

        [Fact]
        public void TestConsumerDecoder()
        {
            // send some messages to each broker
            var sentMessages = this.SendMessagesToBrokerPartition(
                Configs.First(), Topic, 0, nMessages, CompressionCodecs.NoCompressionCodec)
                .Union(
                    this.SendMessagesToBrokerPartition(
                        Configs.First(), Topic, 1, nMessages, CompressionCodecs.NoCompressionCodec))
                .ToList();

            TestUtils.WaitUntilMetadataIsPropagated(this.Servers, Topic, 0, 1000);
            TestUtils.WaitUntilMetadataIsPropagated(this.Servers, Topic, 1, 1000);

            // create a consuemr
            var consumerConfig = TestUtils.CreateConsumerProperties(ZkConnect, Group, Consumer1);

            TestUtils.WaitUntilLeaderIsElectedOrChanged(ZkClient, Topic, 0, 500);
            TestUtils.WaitUntilLeaderIsElectedOrChanged(ZkClient, Topic, 1, 500);

            var zkConsumerConnector = new ZookeeperConsumerConnector(consumerConfig, true);
            var topicMessageStreams =
                zkConsumerConnector.CreateMessageStreams(
                    new Dictionary<string, int> { { Topic, 1 } }, new StringDecoder(), new StringDecoder());

            var receivedMessages = this.GetMessages(nMessages * 2, topicMessageStreams);
            Assert.Equal(sentMessages.OrderBy(x => x).ToArray(), receivedMessages.OrderBy(x => x).ToArray());

            zkConsumerConnector.Shutdown();
        }

        [Fact]
        public void TestLeaderSelectionForPartition()
        {
            var zkClient = new ZkClient(zookeeperConnect, 6000, 30000, new ZkStringSerializer());

            // create topic topic1 with 1 partition on broker 0
            AdminUtils.CreateTopic(zkClient, Topic, 1, 1, new Dictionary<string, string>());

            var sentMessages1 = SendMessages(
                Configs.First(), nMessages, "batch1", CompressionCodecs.NoCompressionCodec, 1);

            TestUtils.WaitUntilMetadataIsPropagated(Servers, Topic, 0, 1000);

            // create a consuemr
            var consumerConfig1 = TestUtils.CreateConsumerProperties(ZkConnect, Group, Consumer1);
            var zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true);
            var topicMessageStreams1 =
                zkConsumerConnector1.CreateMessageStreams(
                    new Dictionary<string, int> { { Topic, 1 } }, new StringDecoder(), new StringDecoder());

            var topicRegistry = zkConsumerConnector1.TopicRegistry;
            Assert.Equal(1, topicRegistry.Select(x => x.Key).Count());
            Assert.Equal(Topic, topicRegistry.Select(x => x.Key).First());

            var topicsAndPartitionsInRegistry =
                topicRegistry.Select(x => Tuple.Create(x.Key, x.Value.Select(p => p.Value))).ToList();

            var brokerPartition = topicsAndPartitionsInRegistry.First().Item2.First();
            Assert.Equal(0, brokerPartition.PartitionId);

            // also check partition ownership
            var actual_1 = this.GetZKChildrenValues(dirs.ConsumerOwnerDir);
            var expected_1 = new List<Tuple<string, string>>
                                 {
                                     Tuple.Create("0", "group1_consumer1-0"),
                                 };
            Assert.Equal(expected_1, actual_1);

            var receivedMessages1 = this.GetMessages(nMessages, topicMessageStreams1);
            Assert.Equal(sentMessages1, receivedMessages1);
            zkConsumerConnector1.Shutdown();
            zkClient.Dispose();
        }

        public List<string> SendMessagesToBrokerPartition(
            TempKafkaConfig config,
            string topic,
            int partition,
            int numMessages,
            CompressionCodecs compression = CompressionCodecs.NoCompressionCodec)
        {
            var header = string.Format("test-{0}-{1}", config.BrokerId, partition);
            var props = new ProducerConfig
                            {
                                Brokers = TestUtils.GetBrokerListFromConfigs(Configs),
                                PartitionerClass = typeof(FixedValuePartitioner).AssemblyQualifiedName,
                                CompressionCodec = compression,
                                KeySerializer = typeof(IntEncoder).AssemblyQualifiedName,
                                Serializer = typeof(StringEncoder).AssemblyQualifiedName,
                                RetryBackoffMs = 1000, //TODO: delete me
                            }; 
            var producer = new Producer<int, string>(props);
            var ms =
                Enumerable.Range(0, numMessages)
                          .Select(x => header + config.BrokerId + "-" + partition + "-" + x)
                          .ToList();
            producer.Send(ms.Select(x => new KeyedMessage<int, string>(topic, partition, x)).ToArray());
            Logger.DebugFormat("Sent {0} messages to broker {1} for partition [{2},{3}]", ms.Count, config.BrokerId, Topic, partition);
            producer.Dispose();
            return ms;
        }

        public List<string> SendMessages(
            TempKafkaConfig config, int messagesPerNode, string header, CompressionCodecs compressionCodec, int numParts)
        {
            var messages = new List<string>();
            var props = new ProducerConfig
            {
                Brokers = TestUtils.GetBrokerListFromConfigs(Configs),
                PartitionerClass = typeof(FixedValuePartitioner).AssemblyQualifiedName,
                KeySerializer = typeof(IntEncoder).AssemblyQualifiedName,
                Serializer = typeof(StringEncoder).AssemblyQualifiedName,
            };
            var producer = new Producer<int, string>(props);
            for (var partition = 0; partition < numParts; partition++)
            {
                var ms =
                    Enumerable.Range(0, messagesPerNode)
                              .Select(x => header + config.BrokerId + "-" + partition + "-" + x)
                              .ToList();
                producer.Send(ms.Select(m => new KeyedMessage<int, string>(Topic, partition, m)).ToArray());
                messages.AddRange(ms);
                Logger.DebugFormat("Sent {0} messages to broker {1} for partition [{2},{3}]", ms.Count, config.BrokerId, Topic, partition);
            }
            producer.Dispose();
            return messages;
        }

        //TODO : sendMessages

        public List<string> GetMessages(
            int nMessagesPerThread, IDictionary<string, IList<KafkaStream<string, string>>> topicMessageStreams)
        {
            var messages = new List<string>();

            foreach (var kvp in topicMessageStreams)
            {
                var topic = kvp.Key;
                var messageStreams = kvp.Value;
                foreach (var messageStream in messageStreams)
                {
                    
                    var iterator = messageStream.GetEnumerator();
                    for (int i = 0; i < nMessagesPerThread; i++)
                    {
                        Assert.True(iterator.MoveNext());
                        var message = iterator.Current.Message;
                        messages.Add(message);
                        Logger.DebugFormat("received message: {0}", message);
                    }
                }
            }
            return messages;
        }
            

        public List<Tuple<string, string>> GetZKChildrenValues(string path)
        {
            var children = ZkClient.GetChildren(path).OrderBy(x => x).ToList();

            return
                children.Select(partition => Tuple.Create(partition, ZkClient.ReadData<string>(path + "/" + partition)))
                        .ToList();

        }

    }
}