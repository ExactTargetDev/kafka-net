namespace Kafka.Tests.Integration
{
    using System;
    using System.Collections.Generic;

    using Kafka.Client.Api;
    using Kafka.Client.Common;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Utils;
    using Kafka.Tests.Custom.Server;
    using Kafka.Tests.Utils;

    using Xunit;

    using System.Linq;

    public class LazyInitProducerTest : ProducerConsumerTestHarness
    {
        protected override List<TempKafkaConfig> CreateConfigs()
        {
            return new List<TempKafkaConfig> { TestUtils.CreateBrokerConfig(0, TestUtils.ChoosePort())};
        }

        public LazyInitProducerTest()
        {
            if (Configs.Count <= 0)
            {
                throw new KafkaException("Must suply at least one server config.");
            }
        }

        [Fact]
        public void TestProduceAndFetch()
        {
            // send some messages
            var topic = "test";
            var sendMessages = new List<string> { "hello", "there" };
            var producerData = sendMessages.Select(m => new KeyedMessage<string, string>(topic, topic, m)).ToArray();

            Producer.Send(producerData);

            TestUtils.WaitUntilMetadataIsPropagated(Servers, topic, 0, 1000);

            ByteBufferMessageSet fetchedMessage = null;
            while (fetchedMessage == null || fetchedMessage.ValidBytes == 0)
            {
                var fetched = Consumer.Fetch(new FetchRequestBuilder().AddFetch(topic, 0, 0, 10000).Build());
                fetchedMessage = fetched.MessageSet(topic, 0);
            }

            Assert.Equal(sendMessages, fetchedMessage.Select(m => Util.ReadString(m.Message.Payload)).ToList());

            // send an invalid offset
            try
            {
                var fetchedWithError = Consumer.Fetch(new FetchRequestBuilder().AddFetch(topic, 0, -1, 10000).Build());
                foreach (var pdata in fetchedWithError.Data.Values)
                {
                    ErrorMapping.MaybeThrowException(pdata.Error);
                }
                Assert.True(false, "Expected an OffsetOutOfRangeException exception to be thrown");
            }
            catch (OffsetOutOfRangeException)
            {
            }
        }

        [Fact]
        public void TestProduceAndMultiFetch()
        {
            // send some messages, with non-ordered topics
            var topicOffsets = new List<Tuple<string, int>> { Tuple.Create("test4", 0), Tuple.Create("test1", 0) , Tuple.Create("test2", 0), Tuple.Create("test3", 0) };

            {
                var messages = new Dictionary<string, List<string>>();
                var builder = new FetchRequestBuilder();
                foreach (var topicAndOffset in topicOffsets)
                {
                    var topic = topicAndOffset.Item1;
                    var offset = topicAndOffset.Item2;

                    var producedData = new List<string> { "a_" + topic, "b_" + topic };
                    messages[topic] = producedData;
                    Producer.Send(producedData.Select(m => new KeyedMessage<string, string>(topic, topic, m)).ToArray());
                    TestUtils.WaitUntilMetadataIsPropagated(Servers, topic, 0, 1000);
                    builder.AddFetch(topic, offset, 0, 10000);
                }

                // wait a bit for produced message to be available
                var request = builder.Build();
                var response = Consumer.Fetch(request);
                foreach (var topicAndOffset in topicOffsets)
                {
                    var fetched = response.MessageSet(topicAndOffset.Item1, topicAndOffset.Item2);
                    Assert.Equal(messages[topicAndOffset.Item1], fetched.Select(m => Util.ReadString(m.Message.Payload)).ToList());
                }
            }

            {
                // send some invalid offsets
                var builder = new FetchRequestBuilder();
                foreach (var topicAndOffset in topicOffsets)
                {
                    builder.AddFetch(topicAndOffset.Item1, topicAndOffset.Item2, -1, 10000);
                }

                var request = builder.Build();
                var responses = Consumer.Fetch(request);
                foreach (var pd in responses.Data.Values)
                {
                    try
                    {
                        ErrorMapping.MaybeThrowException(pd.Error);
                        Assert.True(false, "Expected an OffsetOutOfRangeException exception to be thrown");
                    }
                    catch (OffsetOutOfRangeException)
                    {
                        // this is good
                    }
                }

            }
        }

        [Fact]
        public void TestMultiProduce()
        {
            // send some messages
            var topics = new List<string> { "test1", "test2", "test3" };
            var messages = new Dictionary<string, List<string>>();
            var builder = new FetchRequestBuilder();
            List<KeyedMessage<string, string>> producerList = new List<KeyedMessage<string, string>>();
            foreach (var topic in topics)
            {
                var set = new List<string> { "a_" + topic, "b_" + topic };
                messages[topic] = set;
                producerList.AddRange(set.Select(x => new KeyedMessage<string, string>(topic, topic, x)));
                builder.AddFetch(topic, 0, 0, 10000);
            }

            Producer.Send(producerList.ToArray());
            foreach (var topic in topics)
            {
                TestUtils.WaitUntilMetadataIsPropagated(Servers, topic, 0, 1000);
            }

            // wait a bit for produced message to be available
            var request = builder.Build();
            var response = Consumer.Fetch(request);
            foreach (var topic in topics)
            {
                var fetched = response.MessageSet(topic, 0);
                Assert.Equal(messages[topic], fetched.Select(m => Util.ReadString(m.Message.Payload)).ToList());
            }
        }


        [Fact]
        public void TestMultiProduceResend()
        {
            // send some messages
            var topics = new List<string> { "test1", "test2", "test3" };
            var messages = new Dictionary<string, List<string>>();
            var builder = new FetchRequestBuilder();
            List<KeyedMessage<string, string>> producerList = new List<KeyedMessage<string, string>>();
            foreach (var topic in topics)
            {
                var set = new List<string> { "a_" + topic, "b_" + topic };
                messages[topic] = set;
                producerList.AddRange(set.Select(x => new KeyedMessage<string, string>(topic, topic, x)));
                builder.AddFetch(topic, 0, 0, 10000);
            }

            Producer.Send(producerList.ToArray());
            foreach (var topic in topics)
            {
                TestUtils.WaitUntilMetadataIsPropagated(Servers, topic, 0, 1000);
            }

            Producer.Send(producerList.ToArray());

            // wait a bit for produced message to be available
            var request = builder.Build();
            var response = Consumer.Fetch(request);
            foreach (var topic in topics)
            {
                var fetched = response.MessageSet(topic, 0);
                var mergedList = new List<string>(messages[topic]);
                mergedList.AddRange(messages[topic]);
                Assert.Equal(mergedList, fetched.Select(m => Util.ReadString(m.Message.Payload)).ToList());
            }
        }
    }
}