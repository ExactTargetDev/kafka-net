namespace Kafka.Tests.Consumers
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;

    using Kafka.Client.Admin;
    using Kafka.Client.Cfg;
    using Kafka.Client.Clusters;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Consumers;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Serializers;
    using Kafka.Client.Utils;
    using Kafka.Tests.Custom.Server;
    using Kafka.Tests.Integration;
    using Kafka.Tests.Utils;

    using Xunit;

    using System.Linq;

    public class ConsumerIteratorTest : KafkaServerTestHarness
    {
        private const int NumNodes = 1;

        protected override List<TempKafkaConfig> CreateConfigs()
        {
            return TestUtils.CreateBrokerConfigs(
                NumNodes,
                idx => new Dictionary<string, string>
                           {
                               {"zookeeper.connect", "localhost:" + TestZkUtils.ZookeeperPort }
                           });
        }

        private readonly Dictionary<int, List<Message>> messages = new Dictionary<int, List<Message>>();

        private readonly string topic = "topic";

        private readonly string group = "group1";

        private readonly string consumer0 = "consumer0";

        private readonly int consumedOffset = 5;

        private readonly Cluster cluster;

        private readonly BlockingCollection<FetchedDataChunk> queue = new BlockingCollection<FetchedDataChunk>();

        private readonly List<PartitionTopicInfo> topicInfos;

        private readonly ConsumerConfig consumerConfig;

        public ConsumerIteratorTest()
        {

            //TODO: move to separate class and config statically
            log4net.Config.BasicConfigurator.Configure(
              new log4net.Appender.ConsoleAppender { Layout = new log4net.Layout.SimpleLayout() }
          );

            this.cluster = new Cluster(Configs.Select(c => new Broker(c.BrokerId, "localhost", c.Port)));
            this.topicInfos =
                this.Configs.Select(
                    c =>
                    new PartitionTopicInfo(
                        topic, 0, queue, new AtomicLong(consumedOffset), new AtomicLong(0), new AtomicInteger(0), ""))
                    .ToList();

            this.consumerConfig = TestUtils.CreateConsumerProperties(ZkConnect, group, consumer0);

            AdminUtils.CreateOrUpdateTopicPartitionAssignmentPathInZK(
                this.ZkClient,
                this.topic,
                new Dictionary<int, List<int>> { { 0, new List<int> { Configs.First().BrokerId } } },
                new Dictionary<string, string>());

            TestUtils.WaitUntilLeaderIsElectedOrChanged(ZkClient, topic, 0, 500);
        }

        [Fact]
        public void TestConsumerIteratorDeduplicationDeepIterator()
        {
            var messageStrings = Enumerable.Range(0, 10).Select(x => x.ToString()).ToList();
            var messages = messageStrings.Select(s => new Message(Encoding.UTF8.GetBytes(s))).ToList();
            var messageSet = new ByteBufferMessageSet(
                CompressionCodecs.DefaultCompressionCodec, new AtomicLong(0), messages);

            topicInfos[0].Enqueue(messageSet);
            Assert.Equal(1, queue.Count);

            this.queue.Add(ZookeeperConsumerConnector.ShutdownCommand);

            var iter = new ConsumerIterator<string, string>(queue, consumerConfig.ConsumerTimeoutMs, new StringDecoder(), new StringDecoder(), string.Empty);

            var receivedMessages = Enumerable.Range(0, 5).Select(_ => iter.Next().Message).ToList();

            Assert.False(iter.HasNext());

            Assert.Equal(1, queue.Count); // This is only shutdown comamnd
            Assert.Equal(5, receivedMessages.Count);
            var unconsumed =
                messageSet.Where(x => x.Offset >= consumedOffset).Select(m => Util.ReadString(m.Message.Payload));
            Assert.Equal(unconsumed, receivedMessages);
        }

        [Fact]
        public void TestConsumerIteratorDecodingFailure()
        {
            var messageStrings = Enumerable.Range(0, 10).Select(x => x.ToString()).ToList();
            var messages = messageStrings.Select(s => new Message(Encoding.UTF8.GetBytes(s))).ToList();
            var messageSet = new ByteBufferMessageSet(
                CompressionCodecs.NoCompressionCodec, new AtomicLong(0), messages);

            topicInfos[0].Enqueue(messageSet);
            Assert.Equal(1, queue.Count);

            var iter = new ConsumerIterator<string, string>(
                queue, ConsumerConfig.DefaultConsumerTimeoutMs, new FailDecoder(), new FailDecoder(), string.Empty);

            for (var i = 0; i < 5; i++)
            {
                Assert.True(iter.HasNext());
                var message = iter.Next();

                Assert.Equal(message.Offset, i + consumedOffset);

                Assert.Throws<NotSupportedException>(() => message.Message);
            }
        }
    }

    public class FailDecoder : IDecoder<string>
    {
        public FailDecoder(VerifiableProperties props = null)
         {
         }

        public string FromBytes(byte[] bytes)
        {
            throw new NotSupportedException("This decoder does not work at all..");
        }
    }
}