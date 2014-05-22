namespace Kafka.Tests.Producers
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    using Kafka.Client.Admin;
    using Kafka.Client.Api;
    using Kafka.Client.Common;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Tests.Custom.Server;
    using Kafka.Tests.Integration;
    using Kafka.Tests.Utils;

    using System.Linq;

    using Xunit;

    public class SyncProducerTest : KafkaServerTestHarness
    {
        private byte[] messageBytes = new byte[2];

        protected override List<TempKafkaConfig> CreateConfigs()
        {
            return new List<TempKafkaConfig> { TestUtils.CreateBrokerConfigs(1).First() };
        }

        private readonly string ZookeeperConnect;

        public SyncProducerTest()
        {
            this.ZookeeperConnect = TestZkUtils.ZookeeperConnect;
        }

        [Fact]
        public void TestReachableServer()
        {
            var props = TestUtils.GetSyncProducerConfig(this.Configs[0].Port);
            var producer = new SyncProducer(props);
            var firstStart = DateTime.Now;
            {
                var response =
                    producer.Send(
                        TestUtils.ProduceRequest(
                            "test",
                            0,
                            new ByteBufferMessageSet(
                                CompressionCodecs.NoCompressionCodec, new List<Message> { new Message(messageBytes) }),
                            acks: 1));
                Assert.NotNull(response);
            }

            var firstEnd = DateTime.Now;
            Assert.True(firstEnd - firstStart < TimeSpan.FromMilliseconds(500));
            var secondStart = DateTime.Now;
            {
                var response =
                    producer.Send(
                        TestUtils.ProduceRequest(
                            "test",
                            0,
                            new ByteBufferMessageSet(
                                CompressionCodecs.NoCompressionCodec, new List<Message> { new Message(messageBytes) }),
                                acks: 1));
                Assert.NotNull(response);
            }
            var secondEnd = DateTime.Now;
            Assert.True(secondEnd - secondStart < TimeSpan.FromMilliseconds(500));
            {
                var response =
                    producer.Send(
                        TestUtils.ProduceRequest(
                            "test",
                            0,
                            new ByteBufferMessageSet(
                                CompressionCodecs.NoCompressionCodec, new List<Message> { new Message(this.messageBytes) }),
                                acks: 1));
                Assert.NotNull(response);
            }

        }

        [Fact]
        public void TestEmptyProduceRequest()
        {
            var props = TestUtils.GetSyncProducerConfig(this.Configs[0].Port);

            var correlationId = 0;
            var clientId = SyncProducerConfig.DefaultClientId;
            var acktimeoutMs = SyncProducerConfig.DefaultAckTimeout;
            short ack = 1;
            var emptyResult = new ProducerRequest(
                correlationId, clientId, ack, acktimeoutMs, new Dictionary<TopicAndPartition, ByteBufferMessageSet>());

            var producer = new SyncProducer(props);
            var response = producer.Send(emptyResult);
            Assert.True(response != null);
            Assert.True(!response.HasError() && response.Status.Count() == 0);
        }

        [Fact]
        public void TestMessageSizeTooLarge()
        {
            var props = TestUtils.GetSyncProducerConfig(this.Configs[0].Port);

            var producer = new SyncProducer(props);
            AdminUtils.CreateTopic(this.ZkClient, "test", 1, 1, new Dictionary<string, string>());
            TestUtils.WaitUntilLeaderIsElectedOrChanged(this.ZkClient, "test", 0, 500);
            TestUtils.WaitUntilMetadataIsPropagated(Servers, "test", 0, 2000);

            var message1 = new Message(new byte[Configs[0].MessageMaxBytes + 1]);
            var messageSet1 = new ByteBufferMessageSet(CompressionCodecs.NoCompressionCodec, new List<Message> { message1 });
            var response1 = producer.Send(TestUtils.ProduceRequest("test", 0, messageSet1, acks: 1));

            Assert.Equal(1, response1.Status.Count(kvp => kvp.Value.Error != ErrorMapping.NoError));
            Assert.Equal(ErrorMapping.MessageSizeTooLargeCode, response1.Status[new TopicAndPartition("test", 0)].Error);
            Assert.Equal(-1L, response1.Status[new TopicAndPartition("test", 0)].Offset);

            var safeSize = Configs[0].MessageMaxBytes - Message.MessageOverhead - MessageSet.LogOverhead - 1;
            var message2 = new Message(new byte[safeSize]);
            var messageSet2 = new ByteBufferMessageSet(
                CompressionCodecs.NoCompressionCodec, new List<Message> { message2 });
            var response2 = producer.Send(TestUtils.ProduceRequest("test", 0, messageSet2, acks: 1));

            Assert.Equal(0, response2.Status.Count(kvp => kvp.Value.Error != ErrorMapping.NoError));
            Assert.Equal(ErrorMapping.NoError, response2.Status[new TopicAndPartition("test", 0)].Error);
            Assert.Equal(0, response2.Status[new TopicAndPartition("test", 0)].Offset);
        }

        [Fact]
        public void TestMessageSizeTooLargeWithAckZero()
        {
            var props = TestUtils.GetSyncProducerConfig(this.Configs[0].Port);
            props.RequestRequiredAcks = 0;

            var producer = new SyncProducer(props);
            AdminUtils.CreateTopic(this.ZkClient, "test", 1, 1, new Dictionary<string, string>());
            TestUtils.WaitUntilLeaderIsElectedOrChanged(this.ZkClient, "test", 0, 500);

            // This message will be dropped silently since message size too large.
            producer.Send(
                TestUtils.ProduceRequest(
                    "test",
                    0,
                    new ByteBufferMessageSet(
                        CompressionCodecs.NoCompressionCodec,
                        new List<Message> { new Message(new byte[Configs[0].MessageMaxBytes + 1]) })));

            // Send another message whose size is large enough to exceed the buffer size so
            // the socket buffer will be flushed immediately;
            // this send should fail since the socket has been closed
            try
            {
                producer.Send(
                    TestUtils.ProduceRequest(
                        "test",
                        0,
                        new ByteBufferMessageSet(
                            CompressionCodecs.NoCompressionCodec,
                            new List<Message> { new Message(new byte[Configs[0].MessageMaxBytes + 1]) })));
            }
            catch (IOException)
            {
            }
        }

        [Fact]
        public void TestProduceCorrectlyReceivesResponse()
        {
            var props = TestUtils.GetSyncProducerConfig(this.Configs[0].Port);

            var producer = new SyncProducer(props);
            var messages = new ByteBufferMessageSet(
                CompressionCodecs.NoCompressionCodec, new List<Message> { new Message(this.messageBytes) });

            // #1 - test that we get an error when partition does not belong to broker in response
            var request = TestUtils.ProduceRequestWithAcks(
                new List<string> { "topic1", "topic2", "topic3" }, new List<int> { 0 }, messages, 1);
            var response = producer.Send(request);

            Assert.NotNull(response);
            Assert.Equal(request.CorrelationId, response.CorrelationId);
            Assert.Equal(3, response.Status.Count);

            foreach (var responseStatus in response.Status.Values)
            {
                Assert.Equal(ErrorMapping.UnknownTopicOrPartitionCode, responseStatus.Error);
                Assert.Equal(-1L, responseStatus.Offset);
            }

            // #2 - test that we get correct offsets when partition is owned by broker
            AdminUtils.CreateTopic(this.ZkClient, "topic1", 1, 1, new Dictionary<string, string>());
            AdminUtils.CreateTopic(this.ZkClient, "topic3", 1, 1, new Dictionary<string, string>());
            TestUtils.WaitUntilLeaderIsElectedOrChanged(this.ZkClient, "topic3", 0, 5000);
            TestUtils.WaitUntilMetadataIsPropagated(Servers, "topic3", 0, 2000);

            var response2 = producer.Send(request);
            Assert.NotNull(response2);
            Assert.Equal(request.CorrelationId, response2.CorrelationId);
            Assert.Equal(3, response2.Status.Count);

            // the first and last message should have been accepted by broker
            Assert.Equal(ErrorMapping.NoError, response2.Status[new TopicAndPartition("topic1", 0)].Error);
            Assert.Equal(ErrorMapping.NoError, response2.Status[new TopicAndPartition("topic3", 0)].Error);
            Assert.Equal(0, response2.Status[new TopicAndPartition("topic1", 0)].Offset);
            Assert.Equal(0, response2.Status[new TopicAndPartition("topic3", 0)].Offset);

            // the middle message should have been rejected because broker doesn't lead partition
            Assert.Equal(ErrorMapping.UnknownTopicOrPartitionCode, response2.Status[new TopicAndPartition("topic2", 0)].Error);
            Assert.Equal(-1L, response2.Status[new TopicAndPartition("topic2", 0)].Offset);
        }

        [Fact]
        public void TestProduceRequestWithNoResponse()
        {
            var props = TestUtils.GetSyncProducerConfig(this.Configs[0].Port);

            var correlationId = 0;
            var clientId = SyncProducerConfig.DefaultClientId;
            var ackTimeoutMs = SyncProducerConfig.DefaultAckTimeout;
            var ack = (short)0;
            var emptyRequest = new ProducerRequest(
                correlationId, clientId, ack, ackTimeoutMs, new Dictionary<TopicAndPartition, ByteBufferMessageSet>());
            var producer = new SyncProducer(props);
            var response = producer.Send(emptyRequest);
            Assert.True(response == null);
        }
    }
}