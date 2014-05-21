namespace Kafka.Tests.Integration
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;

    using Kafka.Client.Admin;
    using Kafka.Client.Api;
    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Serializers;
    using Kafka.Client.Utils;
    using Kafka.Client.ZKClient;
    using Kafka.Tests.Custom.Server;
    using Kafka.Tests.Utils;

    using Xunit;

    /// <summary>
    ///  End to end tests of the primitive apis against a local server
    /// </summary>
    public class PrimitiveApiTest : ProducerConsumerTestHarness
    {
        protected override List<TempKafkaConfig> CreateConfigs()
        {
            return new List<TempKafkaConfig> { TestUtils.CreateBrokerConfig(0, TestUtils.ChoosePort()) };
        }        

        [Fact]
        public void TestFetchRequestCanProperlySerialize()
        {
            var request =
                new FetchRequestBuilder().ClientId("test-client")
                                         .MaxWait(10001)
                                         .MinBytes(4444)
                                         .AddFetch("topic1", 0, 0, 10000)
                                         .AddFetch("topic2", 1, 1024, 9999)
                                         .AddFetch("topic1", 1, 256, 444)
                                         .Build();
            var serializedBuilder = ByteBuffer.Allocate(request.SizeInBytes);
            request.WriteTo(serializedBuilder);
            serializedBuilder.Rewind();
            var deserializedRequest = FetchRequest.ReadFrom(serializedBuilder);
            Assert.Equal(request, deserializedRequest);
        }

        [Fact]
        public void TestEmptyFetchRequest()
        {
            var partitionRequests = new Dictionary<TopicAndPartition, PartitionFetchInfo>();
            var request = new FetchRequest(requestInfo: partitionRequests);
            var fetched = Consumer.Fetch(request);
            Assert.True(!fetched.HasError && fetched.Data.Count == 0);
        }

        [Fact]
        public void TestDefaultEncoderProducerAndFetch()
        {
            var topic = "test-topic";
            var config = Producer.Config;

            var stringProducer1 = new Producer<string, string>(config);
            stringProducer1.Send(new KeyedMessage<string, string>(topic, "test-message"));

            // note we can't validate high watermark here
            var request = new FetchRequestBuilder().ClientId("test-client").AddFetch(topic, 0, 0, 10000).Build();
            var fetched = Consumer.Fetch(request);
            Assert.Equal(0, fetched.CorrelationId);

            var messageSet = fetched.MessageSet(topic, 0);
            Assert.True(messageSet.Iterator().HasNext());

            var fetchedMessageAndOffset = messageSet.Iterator().Next();
            Assert.Equal("test-message", Util.ReadString(fetchedMessageAndOffset.Message.Payload));
        }

        [Fact]
        public void TestDefaultEncoderProducerAndFetchWithCompression()
        {
            var topic = "test-topic";
            var config = Producer.Config;
            config.CompressionCodec = CompressionCodecs.DefaultCompressionCodec;
            config.KeySerializer = typeof(StringEncoder).AssemblyQualifiedName;
            config.Serializer = typeof(StringEncoder).AssemblyQualifiedName;

            var stringProducer1 = new Producer<string, string>(config);
            stringProducer1.Send(new KeyedMessage<string, string>(topic, "test-message"));

            // note we can't validate high watermark here
            var request = new FetchRequestBuilder().ClientId("test-client").AddFetch(topic, 0, 0, 10000).Build();
            var fetched = Consumer.Fetch(request);
            Assert.Equal(0, fetched.CorrelationId);

            var messageSet = fetched.MessageSet(topic, 0);
            Assert.True(messageSet.Iterator().HasNext());

            var fetchedMessageAndOffset = messageSet.Iterator().Next();
            Assert.Equal("test-message", Util.ReadString(fetchedMessageAndOffset.Message.Payload));
        }

        [Fact]
        public void TestProduceAndMultiFetch()
        {
            this.CreateSimpleTopicsAndAwaitLeader(
                this.ZkClient, new List<string> { "test1", "test2", "test3", "test4" }, Configs.First().BrokerId);

            // send some messages, with non-ordered topics
            var topics = new List<Tuple<string, int>> { Tuple.Create("test4", 0), Tuple.Create("test1", 0), Tuple.Create("test2", 0), Tuple.Create("test3", 0) };

            {
                var messages = new Dictionary<string, List<string>>();
                var builder = new FetchRequestBuilder();
                foreach (var topicAndOffset in topics)
                {
                    var topic = topicAndOffset.Item1;
                    var partition = topicAndOffset.Item2;

                    var messageList = new List<string> { "a_" + topic, "b_" + topic };
                    var producerData = messageList.Select(m => new KeyedMessage<string, string>(topic, topic, m)).ToArray();
                    messages[topic] = messageList;
                    Producer.Send(producerData);
                    builder.AddFetch(topic, partition, 0, 10000);
                }

                // wait a bit for produced message to be available
                var request = builder.Build();
                var response = Consumer.Fetch(request);
                foreach (var topicAndPartition in topics)
                {
                    var fetched = response.MessageSet(topicAndPartition.Item1, topicAndPartition.Item2);
                    Assert.Equal(messages[topicAndPartition.Item1], fetched.Select(m => Util.ReadString(m.Message.Payload)).ToList());
                }
            }

            {
                // send some invalid offsets
                var builder = new FetchRequestBuilder();
                foreach (var topicAndPartition in topics)
                {
                    builder.AddFetch(topicAndPartition.Item1, topicAndPartition.Item2, -1, 10000);
                }

                try
                {
                    var request = builder.Build();
                    var response = Consumer.Fetch(request);
                    foreach (var pdata in response.Data.Values)
                    {
                        ErrorMapping.MaybeThrowException(pdata.Error);
                    }

                    Assert.True(false, "Expected exception when fetching message with invalid offset");
                }
                catch (OffsetOutOfRangeException)
                {
                    // ok
                }
            }

            {
                // send some invalid partitions
                var builder = new FetchRequestBuilder();
                foreach (var topicAndPartition in topics)
                {
                    builder.AddFetch(topicAndPartition.Item1, -1, 0, 10000);
                }

                try
                {
                    var request = builder.Build();
                    var response = Consumer.Fetch(request);
                    foreach (var pdata in response.Data.Values)
                    {
                        ErrorMapping.MaybeThrowException(pdata.Error);
                    }

                    Assert.True(false, "Expected exception when fetching message with invalid partition");
                }
                catch (UnknownTopicOrPartitionException)
                {
                    // ok
                }
            }
        }

        public void TestMultiProduce()
        {
            this.CreateSimpleTopicsAndAwaitLeader(
               this.ZkClient, new List<string> { "test1", "test2", "test3", "test4" }, Configs.First().BrokerId);

            // send some messages
            var topics = new List<Tuple<string, int>> { Tuple.Create("test4", 0), Tuple.Create("test1", 0), Tuple.Create("test2", 0), Tuple.Create("test3", 0) };

            {
                var messages = new Dictionary<string, List<string>>();
                var builder = new FetchRequestBuilder();
                foreach (var topicAndOffset in topics)
                {
                    var topic = topicAndOffset.Item1;
                    var partition = topicAndOffset.Item2;

                    var messageList = new List<string> { "a_" + topic, "b_" + topic };
                    var producerData = messageList.Select(m => new KeyedMessage<string, string>(topic, topic, m)).ToArray();
                    messages[topic] = messageList;
                    Producer.Send(producerData);
                    builder.AddFetch(topic, partition, 0, 10000);
                }

                // wait a bit for produced message to be available
                var request = builder.Build();
                var response = Consumer.Fetch(request);
                foreach (var topicAndPartition in topics)
                {
                    var fetched = response.MessageSet(topicAndPartition.Item1, topicAndPartition.Item2);
                    Assert.Equal(messages[topicAndPartition.Item1], fetched.Select(m => Util.ReadString(m.Message.Payload)).ToList());
                }
            }
        }

        [Fact]
        public void TestConsumerEmptyTopic()
        {
            var newTopic = "new-topic";
            AdminUtils.CreateTopic(this.ZkClient, newTopic, 1, 1, new Dictionary<string, string>());
            TestUtils.WaitUntilMetadataIsPropagated(this.Servers, newTopic, 0, 1000);
            TestUtils.WaitUntilLeaderIsElectedOrChanged(this.ZkClient, newTopic, 0, 500);
            var fetchResponse = Consumer.Fetch(new FetchRequestBuilder().AddFetch(newTopic, 0, 0, 10000).Build());
            Assert.False(fetchResponse.MessageSet(newTopic, 0).Iterator().HasNext());
        }

        [Fact]
        public void TestPipelinedProduceRequests()
        {
            this.CreateSimpleTopicsAndAwaitLeader(this.ZkClient, new List<string> { "test1", "test2", "test3", "test4" }, Configs.First().BrokerId);
            var props = Producer.Config;
            props.RequestRequiredAcks = 0;

            var pipelinedProducer = new Producer<string, string>(props);

            // send some messages
            var topics = new List<Tuple<string, int>>
                             {
                                 Tuple.Create("test4", 0),
                                 Tuple.Create("test1", 0),
                                 Tuple.Create("test2", 0),
                                 Tuple.Create("test3", 0)
                             };
            var messages = new Dictionary<string, List<string>>();
            var builder = new FetchRequestBuilder();
            foreach (var topicAndPartition in topics)
            {
                var topic = topicAndPartition.Item1;
                var partition = topicAndPartition.Item2;
                var messageList = new List<string> { "a_" + topics, "b_" + topic };
                var producerData = messageList.Select(m => new KeyedMessage<string, string>(topic, topic, m)).ToArray();
                messages[topic] = messageList;
                pipelinedProducer.Send(producerData);
                builder.AddFetch(topic, partition, 0, 10000);
            }

            // wait until the messages are published
            Thread.Sleep(7000); // ugly sleep as we can't access logManager endOffset //TODO: can we reduce this value ?

            var request = builder.Build();
            var response = Consumer.Fetch(request);
            foreach (var topicAndPartition in topics)
            {
                var topic = topicAndPartition.Item1;
                var partition = topicAndPartition.Item2;
                var fetched = response.MessageSet(topic, partition);
                Assert.Equal(messages[topic], fetched.Select(messageAndOffset => Util.ReadString(messageAndOffset.Message.Payload)).ToList());
            }
        }

        public void CreateSimpleTopicsAndAwaitLeader(ZkClient zkClient, List<string> topics, int brokerId)
        {
            foreach (var topic in topics)
            {
                AdminUtils.CreateTopic(zkClient, topic, 1, 1, new Dictionary<string, string>());
                TestUtils.WaitUntilLeaderIsElectedOrChanged(this.ZkClient, topic, 0, 500);
            }
        }
    }
}