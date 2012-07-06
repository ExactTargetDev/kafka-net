namespace Kafka.Client.IntegrationTests
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Producers.Sync;
    using Kafka.Client.Requests;

    using NUnit.Framework;

    [TestFixture]
    public class SyncProducerTests : IntegrationFixtureBase
    {
        [Test]
        public void ProducerRequestAndResponseCorrelationIdAndVersionIdTest()
        {
            int correlationId = 10;
            short expectedServerVersionId = 1;
            short versionId = 5;
            var prodConfig = this.SyncProducerConfig1;

            TopicMetadataRequest topicMetadataRequest = TopicMetadataRequest.Create(new List<string> { CurrentTestTopic });
            IEnumerable<TopicMetadata> topicMetadata = null;

            string payload1 = "kafka 1.";
            byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
            var msg = new Message(payloadData1);

            using (var producer = new SyncProducer(prodConfig))
            {
                topicMetadata = producer.Send(topicMetadataRequest);
            }

            var topicMetadataItem = topicMetadata.ToArray()[0];
            var partitionMetadata = topicMetadataItem.PartitionsMetadata.ToArray()[0];
            var broker = partitionMetadata.Replicas.ToArray()[0];
            prodConfig.BrokerId = broker.Id;
            prodConfig.Host = broker.Host;
            prodConfig.Port = broker.Port;

            using (var producer = new SyncProducer(prodConfig))
            {
                var bufferedMessageSet = new BufferedMessageSet(new List<Message>(new List<Message> { msg }));

                var request = new ProducerRequest(
                    versionId,
                    correlationId,
                    "",
                    0,
                    0,
                    new List<TopicData>()
                        {
                            new TopicData(
                                CurrentTestTopic, new List<PartitionData>() { new PartitionData(partitionMetadata.PartitionId, bufferedMessageSet) })
                        });

                Assert.AreEqual(correlationId, request.CorrelationId);
                Assert.AreEqual(versionId, request.VersionId);

                var producerResponse = producer.Send(request);

                Assert.AreEqual(correlationId, producerResponse.CorrelationId);
                Assert.AreEqual(expectedServerVersionId, producerResponse.VersionId);
            }
        }
    }
}