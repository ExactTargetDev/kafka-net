using NUnit.Framework;
namespace Kafka.Client.IntegrationTests
{
    using Kafka.Client.Cfg;
    using System.Collections.Generic;

    using Kafka.Client.Cluster;
    using Kafka.Client.Producers.Sync;
    using Kafka.Client.Messages;
    using System.Text;
    using Kafka.Client.Requests;
    using Kafka.Client.Consumers;
    using System.Linq;
    using System.Collections.Concurrent;
    using System.Threading;

    [TestFixture]
    public class FetcherTests : IntegrationFixtureBase
    {
        [Test]
        public void TestFetcher()
        {
            var configs = new List<SyncProducerConfiguration>() { this.SyncProducerConfig1, this.SyncProducerConfig2, this.SyncProducerConfig3 };
            var queue = new BlockingCollection<FetchedDataChunk>();

            var cluster = new Cluster(
                configs.Select(x => new Broker(x.BrokerId, x.BrokerId.ToString(), x.Host, x.Port)));
            
            var topicInfos =
                configs.Select(
                    x =>
                    new PartitionTopicInfo(
                        this.CurrentTestTopic, x.BrokerId, new Partition(x.BrokerId, 0), queue, 0, 0, 0));

            var fetcher = new Fetcher(this.ConsumerConfig1, null);
            fetcher.InitConnections(topicInfos, cluster, new List<BlockingCollection<FetchedDataChunk>>());

            var perNode = 2;
            var countExpected = SendMessages(perNode, configs);

            Fetch(queue, countExpected);

            Thread.Sleep(100);
            Assert.AreEqual(0, queue.Count);
            countExpected = this.SendMessages(perNode, configs);
            Fetch(queue, countExpected);
            Thread.Sleep(100);
            Assert.AreEqual(0, queue.Count);
        }

        private void Fetch(BlockingCollection<FetchedDataChunk> queue, int countExpected)
        {
            var count = 0;
            while(true)
            {
                FetchedDataChunk chunk = null;
                queue.TryTake(out chunk, 2000);
                Assert.NotNull(chunk);
                if(chunk != null)
                {
                    foreach (var message in chunk.Messages.Messages)
                    {
                        count++;
                    }
                    if(count == countExpected)
                    {
                        break;
                    }
                }
            }
        }

        private int SendMessages(int messagesPerNode, List<SyncProducerConfiguration> configs)
        {
            var count = 0;
            
            foreach (var syncProducerConfiguration in configs)
            {
                using (var producer = new SyncProducer(syncProducerConfiguration))
                {
                    var messageList = new List<Message>();
                    for (int i = 0; i < messagesPerNode; i++)
                    {
                        string payload1 = "kafka " + i.ToString();
                        byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
                        var msg1 = new Message(payloadData1);
                        messageList.Add(msg1);
                    }

                    var mSet = new BufferedMessageSet(CompressionCodecs.NoCompressionCodec, messageList);
                    var request = new ProducerRequest(this.CurrentTestTopic, 0, mSet);
                    producer.Send(request);
                    count += mSet.Messages.Count();
                }
            }
            return count;
        }
    }
}
