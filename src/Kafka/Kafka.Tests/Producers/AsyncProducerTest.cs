namespace Kafka.Tests.Producers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;

    using Kafka.Client.Api;
    using Kafka.Client.Cfg;
    using Kafka.Client.Clusters;
    using Kafka.Client.Common;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Producers.Async;
    using Kafka.Client.Serializers;
    using Kafka.Client.Utils;
    using Kafka.Tests.Custom.Server;
    using Kafka.Tests.Utils;

    using Xunit;

    using Kafka.Client.Extensions;

    public class AsyncProducerTest : IDisposable
    {
        private List<TempKafkaConfig> props;

        public AsyncProducerTest()
        {
            this.props = TestUtils.CreateBrokerConfigs(1);
        }

        [Fact]
        public void TestProducerQueueSize()
        {
            var config = new ProducerConfig();
            config.KeySerializer = typeof(StringEncoder).AssemblyQualifiedName;
            config.Serializer = typeof(StringEncoder).AssemblyQualifiedName;
            config.Brokers = TestUtils.GetBrokerListFromConfigs(props);
            config.ProducerType = ProducerTypes.Async;
            config.QueueBufferingMaxMessages = 10;
            config.BatchNumMessages = 1;
            config.QueueEnqueueTimeoutMs = 0;

            var produceData = GetProduceData(12);
            var producer = new Producer<string, string>(config, new MockEventHandler());
            try
            {
                // send all 10 messages, should hit the batch size and then reach broker
                producer.Send(produceData.ToArray());
                Assert.False(true, "Queue should be full");
            }
            catch (QueueFullException)
            {
                // expected
            }
            finally
            {
                producer.Dispose();
            }
        }

        [Fact]
        public void TestProduceAfterClosed()
        {
            var config = new ProducerConfig();
            config.Serializer = typeof(StringEncoder).AssemblyQualifiedName;
            config.KeySerializer = typeof(StringEncoder).AssemblyQualifiedName;
            config.Brokers = TestUtils.GetBrokerListFromConfigs(props);
            config.ProducerType = ProducerTypes.Async;
            config.BatchNumMessages = 1;

            var produceData = this.GetProduceData(10);
            var producer = new Producer<string, string>(config);
            producer.Dispose();
            
            try
            {
                producer.Send(produceData);
                Assert.False(true, "should complain that producer is already closed");
            }
            catch (ProducerClosedException)
            {
                // exptected
            }
        }

        [Fact]
        public void TestPartitionAndCollateEvents()
        {
            var producerDataList = new List<KeyedMessage<int, Message>>();

            // use bogus key and partition key override for some messages
            producerDataList.Add(new KeyedMessage<int, Message>("topic1", 0, new Message(Encoding.UTF8.GetBytes("msg1"))));
            producerDataList.Add(new KeyedMessage<int, Message>("topic2", -99, 1, new Message(Encoding.UTF8.GetBytes("msg2"))));
            producerDataList.Add(new KeyedMessage<int, Message>("topic1", 2, new Message(Encoding.UTF8.GetBytes("msg3"))));
            producerDataList.Add(new KeyedMessage<int, Message>("topic1", -101, 3, new Message(Encoding.UTF8.GetBytes("msg4"))));
            producerDataList.Add(new KeyedMessage<int, Message>("topic2", 4, new Message(Encoding.UTF8.GetBytes("msg5"))));

            var broker1 = new Broker(0, "localhost", 9092);
            var broker2 = new Broker(1, "localhost", 9093);

            // form expected partitions metadata
            var partition1Metadata = new PartitionMetadata(0, broker1, new List<Broker> { broker1, broker2 });
            var partition2Metadata = new PartitionMetadata(1, broker2, new List<Broker> { broker1, broker2 });
            var topic1Metadata = new TopicMetadata(
                "topic1", new List<PartitionMetadata> { partition1Metadata, partition2Metadata });
            var topic2Metadata = new TopicMetadata(
                "topic2", new List<PartitionMetadata> { partition1Metadata, partition2Metadata });

            var topicPartitionInfos = new Dictionary<string, TopicMetadata>
                                          {
                                              { "topic1", topic1Metadata },
                                              { "topic2", topic2Metadata }
                                          };
            var intPartitioner = new IntPartitioner();

            var config = new ProducerConfig();
            config.Brokers = TestUtils.GetBrokerListFromConfigs(props);

            var producerPool = new ProducerPool(config);
            var handler = new DefaultEventHandler<int, string>(
                config, intPartitioner, null, new IntEncoder(), producerPool, topicPartitionInfos);

            var topic1Broker1Data = new List<KeyedMessage<int, Message>>
                                        {
                                            new KeyedMessage<int, Message>(
                                                "topic1",
                                                0,
                                                new Message(
                                                Encoding.UTF8.GetBytes("msg1"))),
                                            new KeyedMessage<int, Message>(
                                                "topic1",
                                                2,
                                                new Message(
                                                Encoding.UTF8.GetBytes("msg3")))
                                        };
            var topic1Broker2Data = new List<KeyedMessage<int, Message>>
                                        {
                                            new KeyedMessage<int, Message>(
                                                "topic1",
                                                -101,
                                                3,
                                                new Message(
                                                Encoding.UTF8.GetBytes("msg4")))
                                        };

            var topic2Broker1Data = new List<KeyedMessage<int, Message>>
                                        {
                                            new KeyedMessage<int, Message>(
                                                "topic2",
                                                4,
                                                new Message(
                                                Encoding.UTF8.GetBytes("msg5")))
                                        };
            var topic2Broker2Data = new List<KeyedMessage<int, Message>>
                                        {
                                            new KeyedMessage<int, Message>(
                                                "topic2",
                                                -99,
                                                1,
                                                new Message(
                                                Encoding.UTF8.GetBytes("msg2")))
                                        };

            var expectedResult = new Dictionary<int, Dictionary<TopicAndPartition, List<KeyedMessage<int, Message>>>>
                                      {
                                          { 0, new Dictionary<TopicAndPartition, List<KeyedMessage<int, Message>>>
                                                   {
                                                       { new TopicAndPartition("topic1", 0), topic1Broker1Data },
                                                       { new TopicAndPartition("topic2", 0), topic2Broker1Data }
                                                   }},
                                          { 1, new Dictionary<TopicAndPartition, List<KeyedMessage<int, Message>>>
                                                   {
                                                       { new TopicAndPartition("topic1", 1), topic1Broker2Data },
                                                       { new TopicAndPartition("topic2", 1), topic2Broker2Data }
                                                   }},
                                      };

            var actualResut = handler.PartitionAndCollate(producerDataList);

            Assert.Equal(expectedResult.Count, actualResut.Count);
            Assert.True(expectedResult.Keys.SequenceEqual(actualResut.Keys));
            foreach (var key in expectedResult.Keys)
            {
                var exptectedInnerDict = expectedResult[key];
                var actualInnerDict = actualResut[key];
                Assert.Equal(exptectedInnerDict.Count, actualInnerDict.Count);
                foreach (var topicAndPartition in exptectedInnerDict.Keys)
                {
                    var exptectedKeyedMsgs = exptectedInnerDict[topicAndPartition];
                    var actualKeyedMsgs = actualInnerDict[topicAndPartition];
                    Assert.True(exptectedKeyedMsgs.SequenceEqual(actualKeyedMsgs));
                }
            }
        }

        [Fact]
        public void TestSerializeEvents()
        {
            var produceData =
                TestUtils.GetMsgStrings(5).Select(m => new KeyedMessage<string, string>("topic1", m)).ToList();
            var config = new ProducerConfig();
            config.Brokers = TestUtils.GetBrokerListFromConfigs(props);

            // form expected partitions metadata
            var topic1Metadata = GetTopicMetadata("topic1", 0, 0, "localhost", 9092);
            var topicPartitionInfos = new Dictionary<string, TopicMetadata> { { "topic1", topic1Metadata } };

            var producerPool = new ProducerPool(config);

            var handler = new DefaultEventHandler<string, string>(
                config, null, new StringEncoder(), new StringEncoder(), producerPool, topicPartitionInfos);

            var serializedData = handler.Serialize(produceData);
            var deserializedData =
                serializedData.Select(
                    d => new KeyedMessage<string, string>(d.Topic, Util.ReadString(d.Message.Payload))).ToList();
            TestUtils.CheckEquals(produceData.GetEnumerator(), deserializedData.GetEnumerator());
        }

        [Fact]
        public void TestInvalidPartition()
        {
            var producerDataList = new List<KeyedMessage<string, Message>>();
            producerDataList.Add(new KeyedMessage<string, Message>("topic1", "key1", new Message(Encoding.UTF8.GetBytes("msg1"))));

            var config = new ProducerConfig();
            config.Brokers = TestUtils.GetBrokerListFromConfigs(props);

            // form expected partitions metadata
            var topic1Metadata = GetTopicMetadata("topic1", 0, 0, "localhost", 9092);
            var topicPartitionInfos = new Dictionary<string, TopicMetadata> { { "topic1", topic1Metadata } };

            var producerPool = new ProducerPool(config);

            var handler = new DefaultEventHandler<string, string>(
                config, new NegativePartitioner(), null, null, producerPool, topicPartitionInfos);

            try
            {
                handler.PartitionAndCollate(producerDataList);
            }
            catch
            {
                Assert.False(true, "Should not throw any exception");
            }
        }

        [Fact]
        public void TestNoBroker()
        {
            var config = new ProducerConfig();
            config.Brokers = TestUtils.GetBrokerListFromConfigs(props);

            // create topic metadata with 0 partitions
            var topic1Metadata = new TopicMetadata("topic1", new List<PartitionMetadata>());
            var topicPartitionInfos = new Dictionary<string, TopicMetadata> { { "topic1", topic1Metadata } };

            var producerPool = new ProducerPool(config);

            var producerDataList = new List<KeyedMessage<string, string>>();
            producerDataList.Add(new KeyedMessage<string, string>("topic1", "msg1"));

            var handler = new DefaultEventHandler<string, string>(
                config, null, new StringEncoder(), new StringEncoder(), producerPool, topicPartitionInfos);

            try
            {
                handler.Handle(producerDataList);
                Assert.True(false, "Should fail with FailedToSendMessageException");
            }
            catch (FailedToSendMessageException)
            {
                // expted
            }
        }

        [Fact]
        public void TestIncompatibleEncoder()
        {
            var config = new ProducerConfig();
            config.Brokers = TestUtils.GetBrokerListFromConfigs(props);

            try
            {
                var producer = new Producer<string, string>(config);
                producer.Send(this.GetProduceData(1));
                Assert.False(true, "Should fail with InvalidCastException due to incompatible Encoder");
            }
            catch (InvalidCastException e)
            {
            }
        }

        [Fact]
        public void TestRandomPartitioner()
        {
            var config = new ProducerConfig();
            config.Brokers = TestUtils.GetBrokerListFromConfigs(props);

            // create topic metadata with 0 partitions
            var topic1Metadata = this.GetTopicMetadata("topic1", 0, 0, "localhost", 9092);
            var topic2Metadata = this.GetTopicMetadata("topic2", 0, 0, "localhost", 9092);

            var topicPartitionInfos = new Dictionary<string, TopicMetadata>
                                          {
                                              { "topic1", topic1Metadata },
                                              { "topic2", topic2Metadata }
                                          };

            var producerPool = new ProducerPool(config);
            var handler = new DefaultEventHandler<string, string>(
                config, null, null, null, producerPool, topicPartitionInfos);

            var producerDataList = new List<KeyedMessage<string, Message>>();
            producerDataList.Add(new KeyedMessage<string, Message>("topic1", new Message(Encoding.UTF8.GetBytes("msg1"))));
            producerDataList.Add(new KeyedMessage<string, Message>("topic2", new Message(Encoding.UTF8.GetBytes("msg2"))));
            producerDataList.Add(new KeyedMessage<string, Message>("topic1", new Message(Encoding.UTF8.GetBytes("msg3"))));

            var partitionedDataOpt = handler.PartitionAndCollate(producerDataList);
            if (partitionedDataOpt != null)
            {
                foreach (var brokerAndData in partitionedDataOpt)
                {
                    var brokerId = brokerAndData.Key;
                    var dataPerBroker = brokerAndData.Value;
                    foreach (var topicPartitionData in dataPerBroker)
                    {
                        var partitionId = topicPartitionData.Key.Partiton;
                        Assert.True(partitionId == 0);
                    }
                }
            }
            else
            {
                Assert.False(true, "Failed to collate requests by topic, partition");
            }

        }

        public void Dispose()
        {
            if (this.props != null)
            {
                foreach (var serverConfig in this.props)
                {
                    serverConfig.Dispose();
                }    
            }
        }
        
        private List<KeyedMessage<string, string>> GetProduceData(int nEvents)
        {
            var producerDataList = new List<KeyedMessage<string, string>>();
            for (var i = 0; i < nEvents; i++)
            {
                producerDataList.Add(new KeyedMessage<string, string>("topic1", null, "msg" + i));
            }
            return producerDataList;
        }

        private TopicMetadata GetTopicMetadata(
            string topic, int partiton, int brokerId, string brokerHost, int brokerPort)
        {
            return this.GetTopicMetadata(topic, new List<int> { partiton }, brokerId, brokerHost, brokerPort);
        }

        private TopicMetadata GetTopicMetadata(
            string topic, IEnumerable<int> partition, int brokerId, string brokerHost, int brokerPort)
        {
            var broker1 = new Broker(brokerId, brokerHost, brokerPort);
            return new TopicMetadata(topic, partition.Select(p => new PartitionMetadata(p, broker1, new List<Broker> { broker1 })).ToList());
        }

        public ByteBufferMessageSet MessagesToSet(List<string> messages)
        {
            return new ByteBufferMessageSet(
                CompressionCodecs.NoCompressionCodec,
                messages.Select(m => new Message(Encoding.UTF8.GetBytes(m))).ToList());
        }

        public ByteBufferMessageSet MessagesToSet(byte[] key, List<byte[]> messages)
        {
            return new ByteBufferMessageSet(CompressionCodecs.NoCompressionCodec, messages.Select(m => new Message(key: key, bytes: m)).ToList());
        }
    }

    internal class NegativePartitioner : IPartitioner
    {
        public int Partition(object key, int numPartitions)
        {
            return -1;
        }
    }

    internal class IntPartitioner : IPartitioner
    {
        public int Partition(object key, int numPartitions)
        {
            return (int)key % numPartitions;
        }
    }

    internal class MockEventHandler : IEventHandler<string, string>
    {
        public void Dispose()
        {
        }

        public void Handle(IEnumerable<KeyedMessage<string, string>> events)
        {
            Thread.Sleep(500);
        }
    }
}