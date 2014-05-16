namespace Kafka.Tests.Integration
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    using Kafka.Client.Admin;
    using Kafka.Client.Clusters;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Consumers;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Serializers;
    using Kafka.Tests.Custom.Server;
    using Kafka.Tests.Utils;

    using Xunit;

    public class FetcherTest : KafkaServerTestHarness
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

        private readonly Dictionary<int, List<byte[]>> messages = new Dictionary<int, List<byte[]>>();

        private readonly string topic = "topic";

        private readonly Cluster cluster;

        private readonly FetchedDataChunk shutdown = ZookeeperConsumerConnector.ShutdownCommand;

        private readonly BlockingCollection<FetchedDataChunk> queue = new BlockingCollection<FetchedDataChunk>();

        private readonly List<PartitionTopicInfo> topicInfos;

        private readonly ConsumerFetcherManager fetcher;

        public FetcherTest()
        {

            this.cluster = new Cluster(Configs.Select(c => new Broker(c.BrokerId, "localhost", c.Port)));
            this.topicInfos =
                this.Configs.Select(
                    c =>
                    new PartitionTopicInfo(
                        topic, 0, queue, new AtomicLong(0), new AtomicLong(0), new AtomicInteger(0), string.Empty))
                    .ToList();

            AdminUtils.CreateOrUpdateTopicPartitionAssignmentPathInZK(
                this.ZkClient,
                this.topic,
                new Dictionary<int, List<int>> { { 0, new List<int> { Configs.First().BrokerId } } },
                new Dictionary<string, string>());

            TestUtils.WaitUntilLeaderIsElectedOrChanged(ZkClient, topic, 0, 500);

            this.fetcher = new ConsumerFetcherManager("consumer1", new ConsumerConfig("", 1234, ""), ZkClient);
            fetcher.StopConnections();
            fetcher.StartConnections(topicInfos, cluster);
        }

        public override void Dispose()
        {
            fetcher.StopConnections();
            base.Dispose();
        }

        [Fact]
        public void TestFetcher()
        {
            var perNode = 2;
            var count = this.SendMessages(perNode);

            this.Fetch(count);
            this.AssertQueueEmpty();
            count = this.SendMessages(perNode);
            this.Fetch(count);
            this.AssertQueueEmpty();
        }

        public void AssertQueueEmpty()
        {
            Assert.Equal(0, this.queue.Count);
        }

        public int SendMessages(int messagesPerNode)
        {
            var count = 0;
            foreach (var conf in Configs)
            {
                var producer = TestUtils.CreateProducer(TestUtils.GetBrokerListFromConfigs(Configs), new DefaultEncoder(), new StringEncoder());

                var ms =
                    Enumerable.Range(0, messagesPerNode)
                              .Select(x => Encoding.UTF8.GetBytes(conf.BrokerId * 5 + x.ToString()))
                              .ToList();
                messages[conf.BrokerId] = ms;
                producer.Send(ms.Select(m => new KeyedMessage<string, byte[]>(topic, topic, m)).ToArray());
                producer.Dispose();
                count += ms.Count;
            }
            return count;
        }

        public void Fetch(int expected)
        {
            var count = 0;
            while (true)
            {
                FetchedDataChunk chunk;
                queue.TryTake(out chunk, TimeSpan.FromSeconds(2));
                Assert.NotNull(chunk);
                foreach (var message in chunk.Messages)
                {
                    count++;
                }
                if (count == expected)
                {
                    return;
                }
            }
        }
    }
}