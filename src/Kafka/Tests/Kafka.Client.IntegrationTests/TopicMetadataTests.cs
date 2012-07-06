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
    public class TopicMetadataTests : IntegrationFixtureBase
    {
        [Test]
        public void SyncProducerSendsTopicMetadataRequestWithOneTopic()
        {
            var prodConfig = this.SyncProducerConfig1;
            int waitSingle = 100;
            int totalWaitTimeInMiliseconds = 0;

            var topic = CurrentTestTopic;

            TopicMetadataRequest request = TopicMetadataRequest.Create(new List<string> { topic });

            using (var producer = new SyncProducer(prodConfig))
            {
                var response = producer.Send(request);
                Assert.NotNull(response);
                Assert.AreEqual(1, response.Count());
                var responseItem = response.ToArray()[0];
                Assert.AreEqual(CurrentTestTopic, responseItem.Topic);
                Assert.NotNull(responseItem.PartitionsMetadata);
            }
        }

        [Test]
        public void SyncProducerSendsTopicMetadataRequestWithTwoTopics()
        {
            var prodConfig = this.SyncProducerConfig1;
            int waitSingle = 100;
            int totalWaitTimeInMiliseconds = 0;

            var topic1 = CurrentTestTopic + "_1";
            var topic2 = CurrentTestTopic + "_2";

            var request = TopicMetadataRequest.Create(new List<string>() { topic1, topic2 });

            using (var producer = new SyncProducer(prodConfig))
            {
                var response = producer.Send(request);
                Assert.NotNull(response);
                Assert.AreEqual(2, response.Count());
                var responseItem1 = response.ToArray()[0];
                var responseItem2 = response.ToArray()[1];
                Assert.AreEqual(topic1, responseItem1.Topic);
                Assert.NotNull(responseItem1.PartitionsMetadata);
                Assert.AreEqual(topic2, responseItem2.Topic);
                Assert.NotNull(responseItem2.PartitionsMetadata);
            }
        }

        [Test]
        public void SyncProducerGetsTopicMetadataAndSends1Message()
        {
            var prodConfig = this.SyncProducerConfig1;
            int waitSingle = 100;
            int totalWaitTimeInMiliseconds = 0;

            var topic = CurrentTestTopic;

            // first producing
            string payload1 = "TestData.";
            byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
            var msg1 = new Message(payloadData1);

            TopicMetadataRequest topicMetadataRequest = TopicMetadataRequest.Create(new List<string> { topic });
            IEnumerable<TopicMetadata> topicMetadata = null;

            using (var producer = new SyncProducer(prodConfig))
            {
                topicMetadata = producer.Send(topicMetadataRequest);
                Assert.NotNull(topicMetadata);
            }

            var topicMetadataItem = topicMetadata.ToArray()[0];
            var partitionMetadata = topicMetadataItem.PartitionsMetadata.ToArray()[0];
            var broker = partitionMetadata.Replicas.ToArray()[0];
            prodConfig.BrokerId = broker.Id;
            prodConfig.Host = broker.Host;
            prodConfig.Port = broker.Port;

            using (var producer = new SyncProducer(prodConfig))
            {
                var bufferedMessageSet = new BufferedMessageSet(new List<Message>() { msg1 });
                var topicData = new TopicData(CurrentTestTopic, new List<PartitionData> { new PartitionData(partitionMetadata.PartitionId, bufferedMessageSet) });
                var requestData = new List<TopicData> { topicData };

                var req = new ProducerRequest(-1, string.Empty, 0, 0, requestData);
                var producerResponse = producer.Send(req);
                Assert.NotNull(producerResponse);
                Assert.AreEqual(1, producerResponse.Offsets.Count());
                Assert.Greater(producerResponse.Offsets.ToArray()[0], 0);
            }
        }
    }
}