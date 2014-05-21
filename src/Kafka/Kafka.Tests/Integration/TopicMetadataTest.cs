namespace Kafka.Tests.Integration
{
    using System.Collections.Generic;

    using Kafka.Client.Admin;
    using Kafka.Client.Api;
    using Kafka.Client.Client;
    using Kafka.Client.Clusters;
    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Tests.Custom.Server;
    using Kafka.Tests.Utils;

    using Xunit;

    using System.Linq;

    public class TopicMetadataTest : KafkaServerTestHarness
    {
        protected override List<TempKafkaConfig> CreateConfigs()
        {
            return new List<TempKafkaConfig> { TestUtils.CreateBrokerConfig(0, TestUtils.ChoosePort()) };
        } 

        protected List<Broker> Brokers
        {
            get
            {
                return this.Configs.Select(c => new Broker(c.BrokerId, "localhost", c.Port)).ToList();
            }
        }

        [Fact]
        public void TestTopicMetadataRequest()
        {
            // create topic
            var topic = "test";
            AdminUtils.CreateTopic(this.ZkClient, topic, 1, 1, new Dictionary<string, string>());

            // create a topic metadata request
            var topicMetadataRequest = new TopicMetadataRequest(new List<string> { topic }, 0);

            var serializedMetadataRequest = ByteBuffer.Allocate(topicMetadataRequest.SizeInBytes + 2);
            topicMetadataRequest.WriteTo(serializedMetadataRequest);
            serializedMetadataRequest.Rewind();
            var deserializedMetadataRequest = TopicMetadataRequest.ReadFrom(serializedMetadataRequest);

            Assert.Equal(topicMetadataRequest, deserializedMetadataRequest);
        }

        [Fact]
        public void TestBasicTopicMetadata()
        {
            // create topic
            var topic = "test";
            AdminUtils.CreateTopic(this.ZkClient, topic, 1, 1, new Dictionary<string, string>());
            TestUtils.WaitUntilMetadataIsPropagated(this.Servers, topic, 0, 1000);
            TestUtils.WaitUntilLeaderIsElectedOrChanged(this.ZkClient, topic, 0, 1000);
            var topicsMetadata = ClientUtils.FetchTopicMetadata(
                new HashSet<string> { topic }, this.Brokers, "TopicMetadataTest-testBasicTopicMetadata", 2000, 0)
                       .TopicsMetadata;

            Assert.Equal(ErrorMapping.NoError, topicsMetadata.First().ErrorCode);
            Assert.Equal(ErrorMapping.NoError, topicsMetadata.First().PartitionsMetadata.First().ErrorCode);
            Assert.Equal(1, topicsMetadata.Count);
            Assert.Equal("test", topicsMetadata.First().Topic);
            var partitionMetadata = topicsMetadata.First().PartitionsMetadata;
            Assert.Equal(1, partitionMetadata.Count);
            Assert.Equal(0, partitionMetadata.First().PartitionId);
            Assert.Equal(1, partitionMetadata.First().Replicas.Count());
        }

        [Fact]
        public void TestGetAllTopicMetadata()
        {
            // create topic
            var topic1 = "testGetAllTopicMetadata1";
            var topic2 = "testGetAllTopicMetadata2";

            AdminUtils.CreateTopic(ZkClient, topic1, 1, 1, new Dictionary<string, string>());
            AdminUtils.CreateTopic(ZkClient, topic2, 1, 1, new Dictionary<string, string>());

            // wait for leader to be elected for both topics
            TestUtils.WaitUntilMetadataIsPropagated(this.Servers, topic1, 0, 1000);
            TestUtils.WaitUntilMetadataIsPropagated(this.Servers, topic2, 0, 1000);

            // issue metadata request with empty list of topics
            var topicsMetadata =
                ClientUtils.FetchTopicMetadata(
                    new HashSet<string>(), Brokers, "TopicMetadataTest-testGetAllTopicMetadata", 2000, 0).TopicsMetadata;

            Assert.Equal(ErrorMapping.NoError, topicsMetadata.First().ErrorCode);
            Assert.Equal(2, topicsMetadata.Count);
            Assert.Equal(ErrorMapping.NoError, topicsMetadata.First().PartitionsMetadata.First().ErrorCode);
            Assert.Equal(ErrorMapping.NoError, topicsMetadata.Last().PartitionsMetadata.First().ErrorCode);

            var partitionMetadataTopic1 = topicsMetadata.First().PartitionsMetadata;
            var partitionMetadataTopic2 = topicsMetadata.Last().PartitionsMetadata;
            Assert.Equal(1, partitionMetadataTopic1.Count);
            Assert.Equal(0, partitionMetadataTopic1.First().PartitionId);
            Assert.Equal(1, partitionMetadataTopic1.First().Replicas.Count());
            Assert.Equal(1, partitionMetadataTopic2.Count);
            Assert.Equal(0, partitionMetadataTopic2.First().PartitionId);
            Assert.Equal(1, partitionMetadataTopic2.First().Replicas.Count());
        }

        [Fact]
        public void TestAutoCreateTopic()
        {
            // auto create topic
            var topic = "testAutoCreateTopic";
            var topicsMetadata =
                ClientUtils.FetchTopicMetadata(
                    new HashSet<string>{ topic }, Brokers, "TopicMetadataTest-testAutoCreateTopic", 2000, 0)
                           .TopicsMetadata;
            Assert.Equal(ErrorMapping.LeaderNotAvailableCode, topicsMetadata.First().ErrorCode);
            Assert.Equal(1, topicsMetadata.Count);
            Assert.Equal(topic, topicsMetadata.First().Topic);
            Assert.Equal(0, topicsMetadata.First().PartitionsMetadata.Count);

            // wait for leader to be elected
            TestUtils.WaitUntilLeaderIsElectedOrChanged(ZkClient, topic, 0, 1000);
            TestUtils.WaitUntilMetadataIsPropagated(Servers, topic, 0, 1000);

            // retry the metadata for the auto created topic
            topicsMetadata =
                ClientUtils.FetchTopicMetadata(
                    new HashSet<string> { topic }, Brokers, "TopicMetadataTest-testBasicTopicMetadata", 2000, 0)
                           .TopicsMetadata;
            Assert.Equal(ErrorMapping.NoError, topicsMetadata.First().ErrorCode);
            Assert.Equal(ErrorMapping.NoError, topicsMetadata.First().PartitionsMetadata.First().ErrorCode);
            var partitionMetadata = topicsMetadata.First().PartitionsMetadata;
            Assert.Equal(1, partitionMetadata.Count);
            Assert.Equal(0, partitionMetadata.First().PartitionId);
            Assert.Equal(1, partitionMetadata.First().Replicas.Count());
            Assert.True(partitionMetadata.First().Leader != null);
        }
    }
}