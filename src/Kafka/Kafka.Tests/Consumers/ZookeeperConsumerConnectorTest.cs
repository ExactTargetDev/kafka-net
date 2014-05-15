namespace Kafka.Tests.Consumers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Threading;

    using Kafka.Client.Consumers;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Serializers;
    using Kafka.Client.Utils;
    using Kafka.Tests.Custom.Server;
    using Kafka.Tests.Integration;
    using Kafka.Tests.Utils;

    using Xunit;

    using log4net;

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
              new log4net.Appender.ConsoleAppender { Layout = new log4net.Layout.SimpleLayout() }
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

            Thread.Sleep(2000); //TODO: dlete me

            // send some messages to each broker
            var sentMessages1 =
                SendMessagesToBrokerPartition(Configs.First(), Topic, 0, nMessages)
                .Union(SendMessagesToBrokerPartition(Configs.Last(), Topic, 0, nMessages)).ToList();

            // wait to make sure the topic and partition have a leader for the successful case
            TestUtils.WaitUntilLeaderIsElectedOrChanged(ZkClient, Topic, 0, 500); 
            TestUtils.WaitUntilLeaderIsElectedOrChanged(ZkClient, Topic, 1, 500);

            TestUtils.WaitUntilMetadataIsPropagated(Servers, Topic, 0, 1000);
            TestUtils.WaitUntilMetadataIsPropagated(Servers, Topic, 1, 1000);

            // create a consuemr
            var consumerConfig1 = TestUtils.CreateConsumerProperties(ZkConnect, Group, Consumer1);
            var zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true);
            var topicMessageStreams1 =
                zkConsumerConnector1.CreateMessageStreams(
                    new Dictionary<string, int> { { Topic, 1 } }, new StringDecoder(), new StringDecoder());

            var receivedMessages1 = this.GetMessages(nMessages * 2, topicMessageStreams1);
            Assert.Equal(sentMessages1.OrderBy(x => x).ToArray(), receivedMessages1.OrderBy(x => x).ToArray());

            if (1 == 1) //TODO: dlete me
            {
                throw new Exception();
            }

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

            /*
             * TODO: finsih me


    val consumerConfig2 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumer2)) {
      override val rebalanceBackoffMs = RebalanceBackoffMs
    }
    val zkConsumerConnector2 = new ZookeeperConsumerConnector(consumerConfig2, true)
    val topicMessageStreams2 = zkConsumerConnector2.createMessageStreams(Map(topic -> 1), new StringDecoder(), new StringDecoder())
    // send some messages to each broker
    val sentMessages2 = sendMessagesToBrokerPartition(configs.head, topic, 0, nMessages) ++
                        sendMessagesToBrokerPartition(configs.last, topic, 1, nMessages)

    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 500)
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 1, 500)

    val receivedMessages2 = getMessages(nMessages, topicMessageStreams1) ++ getMessages(nMessages, topicMessageStreams2)
    assertEquals(sentMessages2.sorted, receivedMessages2.sorted)

    // also check partition ownership
    val actual_2 = getZKChildrenValues(dirs.consumerOwnerDir)
    val expected_2 = List( ("0", "group1_consumer1-0"),
                           ("1", "group1_consumer2-0"))
    assertEquals(expected_2, actual_2)

    // create a consumer with empty map
    val consumerConfig3 = new ConsumerConfig(
      TestUtils.createConsumerProperties(zkConnect, group, consumer3))
    val zkConsumerConnector3 = new ZookeeperConsumerConnector(consumerConfig3, true)
    val topicMessageStreams3 = zkConsumerConnector3.createMessageStreams(new mutable.HashMap[String, Int]())
    // send some messages to each broker
    val sentMessages3 = sendMessagesToBrokerPartition(configs.head, topic, 0, nMessages) ++
                        sendMessagesToBrokerPartition(configs.last, topic, 1, nMessages)

    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 500)
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 1, 500)

    val receivedMessages3 = getMessages(nMessages, topicMessageStreams1) ++ getMessages(nMessages, topicMessageStreams2)
    assertEquals(sentMessages3.sorted, receivedMessages3.sorted)

    // also check partition ownership
    val actual_3 = getZKChildrenValues(dirs.consumerOwnerDir)
    assertEquals(expected_2, actual_3)

    zkConsumerConnector1.shutdown
    zkConsumerConnector2.shutdown
    zkConsumerConnector3.shutdown
    info("all consumer connectors stopped")
    requestHandlerLogger.setLevel(Level.ERROR)*/

        }

        //TODO: other tests

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
                                Serializer = typeof(StringEncoder).AssemblyQualifiedName
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

        //TODO :sendMessages
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