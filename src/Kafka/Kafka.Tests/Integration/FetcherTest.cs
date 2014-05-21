namespace Kafka.Tests.Integration
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Text;

    using Kafka.Client.Admin;
    using Kafka.Client.Clusters;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Consumers;
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
                               { "zookeeper.connect", "localhost:" + TestZkUtils.ZookeeperPort }
                           });
        }

        private readonly Dictionary<int, List<byte[]>> messages = new Dictionary<int, List<byte[]>>();

        private readonly string topic = "topic";

        private readonly Cluster cluster;

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
                        this.topic, 0, this.queue, new AtomicLong(0), new AtomicLong(0), new AtomicInteger(0), string.Empty))
                    .ToList();

            AdminUtils.CreateOrUpdateTopicPartitionAssignmentPathInZK(
                this.ZkClient,
                this.topic,
                new Dictionary<int, List<int>> { { 0, new List<int> { Configs.First().BrokerId } } },
                new Dictionary<string, string>());

            TestUtils.WaitUntilLeaderIsElectedOrChanged(this.ZkClient, this.topic, 0, 500);

            this.fetcher = new ConsumerFetcherManager("consumer1", new ConsumerConfig(string.Empty, 1234, string.Empty), ZkClient);
            this.fetcher.StopConnections();
            this.fetcher.StartConnections(this.topicInfos, this.cluster);
        }

        public override void Dispose()
        {
            this.fetcher.StopConnections();
            base.Dispose();
        }

        [Fact]
        public void TestFetcher()
        {
            int perNode = 2;
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
            foreach (var conf in this.Configs)
            {
                var producer = TestUtils.CreateProducer(TestUtils.GetBrokerListFromConfigs(Configs), new DefaultEncoder(), new StringEncoder());

                var ms =
                    Enumerable.Range(0, messagesPerNode)
                              .Select(x => Encoding.UTF8.GetBytes((conf.BrokerId * 5) + x.ToString(CultureInfo.InvariantCulture)))
                              .ToList();
                this.messages[conf.BrokerId] = ms;
                producer.Send(ms.Select(m => new KeyedMessage<string, byte[]>(this.topic, this.topic, m)).ToArray());
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
                this.queue.TryTake(out chunk, TimeSpan.FromSeconds(2));
                Assert.NotNull(chunk);
                count += chunk.Messages.Count();
                if (count == expected)
                {
                    return;
                }
            }
        }
    }
}