/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Kafka.Client.IntegrationTests
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using Kafka.Client.Consumers;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Messages;
    using Kafka.Client.Requests;
    using Kafka.Client.Serialization;

    using NUnit.Framework;

    [TestFixture]
    public class CompressionTests : IntegrationFixtureBase
    {
        /// <summary>
        /// Maximum amount of time to wait trying to get a specific test message from Kafka server (in miliseconds)
        /// </summary>
        private static readonly int MaxTestWaitTimeInMiliseconds = 5000;

        [Test]
        [ExpectedException(typeof(UnknownCodecException))]
        public void NoCompressionTest()
        {
            this.SimpleSyncProducerSendsMessagesUsingCompressionAndConsumerGetsThemBack(5, CompressionCodecs.NoCompressionCodec);
        }

        [Test]
        public void GzipCompressionForSingleTopicTest()
        {
            this.SimpleSyncProducerSendsMessagesUsingCompressionAndConsumerGetsThemBack(5, CompressionCodecs.GZIPCompressionCodec);
        }

        [Test]
        public void DefaultCompressionForSingleTopicTest()
        {
            this.SimpleSyncProducerSendsMessagesUsingCompressionAndConsumerGetsThemBack(5, CompressionCodecs.DefaultCompressionCodec);
        }

        [Test]
        public void SnappyCompressionForSingleTopicTest()
        {
            this.SimpleSyncProducerSendsMessagesUsingCompressionAndConsumerGetsThemBack(5, CompressionCodecs.SnappyCompressionCodec);
        }

        [Test]
        public void GzipCompressionForTwoTopicsTest()
        {
            this.SimpleSyncProducerSendsMessagesToDifferentTopicsUsingCompressionAndConsumerGetsThemBack(5, CompressionCodecs.GZIPCompressionCodec);
        }

        [Test]
        public void DefaultCompressionForTwoTopicsTest()
        {
            this.SimpleSyncProducerSendsMessagesToDifferentTopicsUsingCompressionAndConsumerGetsThemBack(5, CompressionCodecs.DefaultCompressionCodec);
        }

        [Test]
        public void SnappyCompressionForTwoTopicsTest()
        {
            this.SimpleSyncProducerSendsMessagesToDifferentTopicsUsingCompressionAndConsumerGetsThemBack(5, CompressionCodecs.SnappyCompressionCodec);
        }

        [Test]
        public void GzipCompressionForTwoPartitionsTest()
        {
            this.SimpleSyncProducerSendsMessagesToDifferentPartitionsUsingCompressionAndConsumerGetsThemBack(5, CompressionCodecs.GZIPCompressionCodec);
        }

        [Test]
        public void DefaultCompressionForTwoPartitionsTest()
        {
            this.SimpleSyncProducerSendsMessagesToDifferentPartitionsUsingCompressionAndConsumerGetsThemBack(5, CompressionCodecs.DefaultCompressionCodec);
        }

        [Test]
        public void SnappyCompressionForTwoPartitionsTest()
        {
            this.SimpleSyncProducerSendsMessagesToDifferentPartitionsUsingCompressionAndConsumerGetsThemBack(5, CompressionCodecs.SnappyCompressionCodec);
        }

        private void SimpleSyncProducerSendsMessagesUsingCompressionAndConsumerGetsThemBack(int numberOfMessages, CompressionCodecs compressionCodec)
        {
            var producerHelper = new ProducerHelper();
            var topic = this.CurrentTestTopic;
            var producerConfiguration = this.SyncProducerConfig1;

            var messages = new List<Message>();
            for (var i = 0; i < numberOfMessages; i++)
            {
                messages.Add(new Message(Encoding.UTF8.GetBytes(string.Format("TestMessage.{0}", i))));
            }

            var compressedMessage = CompressionUtils.Compress(messages, compressionCodec);

            var producerResult = producerHelper.SendMessagesToTopicSynchronously(topic, new List<Message> { compressedMessage }, producerConfiguration);

            var producerResponse = producerResult.Item1;
            var partitionMetadata = producerResult.Item2;

            var broker = partitionMetadata.Replicas.ToArray()[0];

            var consumerConfiguration = this.ConsumerConfig1;
            consumerConfiguration.Broker.BrokerId = broker.Id;
            consumerConfiguration.Broker.Host = broker.Host;
            consumerConfiguration.Broker.Port = broker.Port;

            var consumer = new Consumer(consumerConfiguration);
            var offsetInfo = new List<OffsetDetail> { new OffsetDetail(topic, new List<int>() { partitionMetadata.PartitionId }, new List<long> { 0 }, new List<int> { 256 }) };
            var fetchRequest = new FetchRequest(FetchRequest.CurrentVersion, 0, "", broker.Id, 0, 0, offsetInfo);

            // giving the kafka server some time to process the message
            Thread.Sleep(750 * numberOfMessages);

            var fetchResponse = consumer.Fetch(fetchRequest);
            var fetchedMessageSet = fetchResponse.MessageSet(topic, partitionMetadata.PartitionId);
            var fetchedMessages = fetchedMessageSet.ToList();
            Assert.AreEqual(numberOfMessages, messages.Count);

            for (var i = 0; i < fetchedMessages.Count; i++)
            {
                var message = fetchedMessages[i];
                var expectedPayload = Encoding.UTF8.GetBytes(string.Format("TestMessage.{0}", i));
                var actualPayload = message.Message.Payload;

                Assert.AreEqual(expectedPayload, actualPayload);
            }
        }

        private void SimpleSyncProducerSendsMessagesToDifferentPartitionsUsingCompressionAndConsumerGetsThemBack(int numberOfMessages, CompressionCodecs compressionCodec)
        {
            var producerHelper = new ProducerHelper();
            var topic = this.CurrentTestTopic;
            var producerConfiguration = this.SyncProducerConfig1;

            var messages1 = new List<Message>();
            for (var i = 0; i < numberOfMessages; i++)
            {
                messages1.Add(new Message(Encoding.UTF8.GetBytes(string.Format("TestMessage.{0}", i))));
            }

            var messages2 = new List<Message>();
            for (var i = 0; i < numberOfMessages; i++)
            {
                messages2.Add(new Message(Encoding.UTF8.GetBytes(string.Format("TestMessage.{0}", i))));
            }

            var compressedMessage1 = CompressionUtils.Compress(messages1, compressionCodec);
            var compressedMessage2 = CompressionUtils.Compress(messages2, compressionCodec);

            var producerResult1 = producerHelper.SendMessagesToTopicSynchronously(topic, new List<Message> { compressedMessage1 }, producerConfiguration, 0);

            // giving the kafka server some time to process the message
            Thread.Sleep(750 * numberOfMessages);

            var producerResult2 = producerHelper.SendMessagesToTopicSynchronously(topic, new List<Message> { compressedMessage2 }, producerConfiguration, 1);

            var consumerConfiguration = this.ZooKeeperBasedConsumerConfig;

            // giving the kafka server some time to process the message
            Thread.Sleep(750 * numberOfMessages);

            var resultMessages = new List<Message>();
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfiguration, true))
            {
                var topicCount = new Dictionary<string, int> { { topic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount, new DefaultDecoder());

                Assert.IsTrue(messages.ContainsKey(topic));

                var sets = messages[topic];
                try
                {
                    foreach (var set in sets)
                    {
                        foreach (var message in set)
                        {
                            resultMessages.Add(message);
                        }
                    }
                }
                catch (ConsumerTimeoutException)
                {
                    // do nothing, this is expected
                }
            }

            Assert.AreEqual(numberOfMessages * 2, resultMessages.Count);

            var resultHelper = new Dictionary<string, int>();
            for (var i = 0; i < numberOfMessages; i++)
            {
                resultHelper.Add(string.Format("TestMessage.{0}", i), 0);
            }

            foreach (var message in resultMessages)
            {
                var actualPayload = message.Payload;

                resultHelper[Encoding.UTF8.GetString(actualPayload)] += 1;
            }

            foreach (var key in resultHelper.Keys)
            {
                Assert.AreEqual(2, resultHelper[key]);
            }
        }

        private void SimpleSyncProducerSendsMessagesToDifferentTopicsUsingCompressionAndConsumerGetsThemBack(int numberOfMessages, CompressionCodecs compressionCodec)
        {
            var producerHelper = new ProducerHelper();
            var topic1 = this.CurrentTestTopic + 1;
            var topic2 = this.CurrentTestTopic + 2;
            var producerConfiguration = this.SyncProducerConfig1;

            var messages1 = new List<Message>();
            for (var i = 0; i < numberOfMessages; i++)
            {
                messages1.Add(new Message(Encoding.UTF8.GetBytes(string.Format("1.TestMessage.{0}", i))));
            }

            var messages2 = new List<Message>();
            for (var i = 0; i < numberOfMessages; i++)
            {
                messages2.Add(new Message(Encoding.UTF8.GetBytes(string.Format("2.TestMessage.{0}", i))));
            }

            var compressedMessage1 = CompressionUtils.Compress(messages1, compressionCodec);
            var compressedMessage2 = CompressionUtils.Compress(messages2, compressionCodec);

            var producerResult1 = producerHelper.SendMessagesToTopicSynchronously(topic1, new List<Message> { compressedMessage1 }, producerConfiguration);
            var producerResult2 = producerHelper.SendMessagesToTopicSynchronously(topic2, new List<Message> { compressedMessage2 }, producerConfiguration);

            var consumerConfiguration = this.ZooKeeperBasedConsumerConfig;

            // giving the kafka server some time to process the message
            Thread.Sleep(750 * numberOfMessages * 2);

            var resultMessages1 = new List<Message>();
            var resultMessages2 = new List<Message>();
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfiguration, true))
            {
                var topicCount = new Dictionary<string, int> { { topic1, 1 }, { topic2, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount, new DefaultDecoder());

                Assert.IsTrue(messages.ContainsKey(topic1));
                Assert.IsTrue(messages.ContainsKey(topic2));

                var sets1 = messages[topic1];
                try
                {
                    foreach (var set in sets1)
                    {
                        foreach (var message in set)
                        {
                            resultMessages1.Add(message);
                        }
                    }
                }
                catch (ConsumerTimeoutException)
                {
                    // do nothing, this is expected
                }

                var sets2 = messages[topic2];
                try
                {
                    foreach (var set in sets2)
                    {
                        foreach (var message in set)
                        {
                            resultMessages2.Add(message);
                        }
                    }
                }
                catch (ConsumerTimeoutException)
                {
                    // do nothing, this is expected
                }
            }

            Assert.AreEqual(numberOfMessages, resultMessages1.Count);
            Assert.AreEqual(numberOfMessages, resultMessages2.Count);

            for (var i = 0; i < resultMessages1.Count; i++)
            {
                var message1 = resultMessages1[i];
                var expectedPayload1 = Encoding.UTF8.GetBytes(string.Format("1.TestMessage.{0}", i));
                var actualPayload1 = message1.Payload;

                var message2 = resultMessages2[i];
                var expectedPayload2 = Encoding.UTF8.GetBytes(string.Format("2.TestMessage.{0}", i));
                var actualPayload2 = message2.Payload;

                Assert.AreEqual(expectedPayload1, actualPayload1);
                Assert.AreEqual(expectedPayload2, actualPayload2);
            }
        }

        //[Test]
        //public void AsyncProducerSendsCompressedAndConsumerReceivesSingleSimpleMessage()
        //{
        //    var prodConfig = this.AsyncProducerConfig1;
        //    var consumerConfig = this.ConsumerConfig1;

        //    var sourceMessage = new Message(Encoding.UTF8.GetBytes("test message"));
        //    var compressedMessage = CompressionUtils.Compress(new List<Message> { sourceMessage }, CompressionCodecs.GZIPCompressionCodec);

        //    long currentOffset = TestHelper.GetCurrentKafkaOffset(CurrentTestTopic, consumerConfig);
        //    using (var producer = new AsyncProducer(prodConfig))
        //    {
        //        var producerRequest = new ProducerRequest(
        //            CurrentTestTopic, 0, new List<Message> { compressedMessage });
        //        producer.Send(producerRequest);
        //    }

        //    IConsumer consumer = new Consumer(consumerConfig);
        //    var request = new FetchRequest(CurrentTestTopic, 0, currentOffset);

        //    BufferedMessageSet response;
        //    int totalWaitTimeInMiliseconds = 0;
        //    int waitSingle = 100;
        //    while (true)
        //    {
        //        Thread.Sleep(waitSingle);
        //        response = consumer.Fetch(request);
        //        if (response != null && response.Messages.Count() > 0)
        //        {
        //            break;
        //        }

        //        totalWaitTimeInMiliseconds += waitSingle;
        //        if (totalWaitTimeInMiliseconds >= MaxTestWaitTimeInMiliseconds)
        //        {
        //            break;
        //        }
        //    }

        //    Assert.NotNull(response);
        //    Assert.AreEqual(1, response.Messages.Count());
        //    Message resultMessage = response.Messages.First();
        //    Assert.AreEqual(compressedMessage.ToString(), resultMessage.ToString());
        //}
    }
}
