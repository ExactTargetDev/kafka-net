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

using System.Linq;
using Kafka.Client.Serialization;

namespace Kafka.Client.IntegrationTests
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Text;
    using System.Threading;
    using Kafka.Client.Consumers;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers.Sync;
    using Kafka.Client.Requests;
    using NUnit.Framework;

    [TestFixture]
    public class ConsumerTests : IntegrationFixtureBase
    {
        [Test]
        public void ConsumerConnectorIsCreatedConnectsDisconnectsAndShutsDown()
        {
            var config = this.ZooKeeperBasedConsumerConfig;
            using (new ZookeeperConsumerConnector(config, true))
            {
            }
        }

        [Test]
        public void GetCurrentKafkaOffset()
        {
            var config = this.ZooKeeperBasedConsumerConfig;
            var topic = "topic1";
            var partition = 0;
            var time = -2;
            var maxNemOffsets = 1;
            var request = new OffsetRequest(topic, partition, time, maxNemOffsets);
            IConsumer consumer = new Consumer(config, "192.168.1.40", 9092);
            IList<long> list = consumer.GetOffsetsBefore(request);
            int a = 5;
        }

        [Test]
        public void SimpleSyncProducerSends1MessageAndConsumerGetsItBack()
        {
            this.SimpleSyncProducerSendsMessagesAndConsumerGetsThemBack(1);
        }

        [Test]
        public void SimpleSyncProducerSends2MessagesAndConsumerGetsThemBack()
        {
            this.SimpleSyncProducerSendsMessagesAndConsumerGetsThemBack(2);
        }

        [Test]
        public void SimpleSyncProducerSends10MessagesAndConsumerGetsThemBack()
        {
            this.SimpleSyncProducerSendsMessagesAndConsumerGetsThemBack(10);
        }

        [Test]
        public void SimpleSyncProducerSends20MessagesAndConsumerGetsThemBack()
        {
            this.SimpleSyncProducerSendsMessagesAndConsumerGetsThemBack(20);
        }

        [Test]
        public void FetchRequestAndResponseCorrelationIdAndVersionIdTest()
        {
            short versionId = 5;
            short expectedServerVersionId = 1;
            int correlationId = 10;

            var producerHelper = new ProducerHelper();
            var topic = this.CurrentTestTopic;
            var producerConfiguration = this.SyncProducerConfig1;

            var messages = new List<Message>();
            for (var i = 0; i < 1; i++)
            {
                messages.Add(new Message(Encoding.UTF8.GetBytes(string.Format("TestMessage.{0}", i))));
            }

            var producerResult = producerHelper.SendMessagesToTopicSynchronously(topic, messages, producerConfiguration);

            var producerResponse = producerResult.Item1;
            var partitionMetadata = producerResult.Item2;

            var broker = partitionMetadata.Replicas.ToArray()[0];

            var consumerConfiguration = this.ConsumerConfig1;
            consumerConfiguration.Broker.BrokerId = broker.Id;
            consumerConfiguration.Broker.Host = broker.Host;
            consumerConfiguration.Broker.Port = broker.Port;

            var consumer = new Consumer(consumerConfiguration);
            var offsetInfo = new List<OffsetDetail> { new OffsetDetail(topic, new List<int>() { partitionMetadata.PartitionId }, new List<long> { 0 }, new List<int> { 256 }) };
            var fetchRequest = new FetchRequest(versionId, correlationId, "", broker.Id, 0, 0, offsetInfo);

            Assert.AreEqual(correlationId, fetchRequest.CorrelationId);
            Assert.AreEqual(versionId, fetchRequest.VersionId);

            // giving the kafka server some time to process the message
            Thread.Sleep(750 * 1);

            var fetchResponse = consumer.Fetch(fetchRequest);

            Assert.AreEqual(correlationId, fetchResponse.CorrelationId);
            Assert.AreEqual(expectedServerVersionId, fetchResponse.VersionId);
        }

        private void SimpleSyncProducerSendsMessagesAndConsumerGetsThemBack(int numberOfMessages)
        {
            var producerHelper = new ProducerHelper();
            var topic = this.CurrentTestTopic;
            var producerConfiguration = this.SyncProducerConfig1;

            var messages = new List<Message>();
            for (var i = 0; i < numberOfMessages; i++)
            {
                messages.Add(new Message(Encoding.UTF8.GetBytes(string.Format("TestMessage.{0}", i))));
            }

            var producerResult = producerHelper.SendMessagesToTopicSynchronously(topic, messages, producerConfiguration);

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

        [Test]
        public void SimpleSyncProducerSends2MessagesAndConsumerConnectorGetsThemBack()
        {
            var prodConfig = this.SyncProducerConfig1;
            var consumerConfig = this.ZooKeeperBasedConsumerConfig;

            string payload1 = "kafka 1.";
            byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
            var msg1 = new Message(payloadData1);

            string payload2 = "kafka 2.";
            byte[] payloadData2 = Encoding.UTF8.GetBytes(payload2);
            var msg2 = new Message(payloadData2);

            var topic = CurrentTestTopic;

            var producerHelper = new ProducerHelper();
            producerHelper.SendMessagesToTopicSynchronously(topic, new List<Message>() { msg1, msg2 }, prodConfig);

            // now consuming
            var resultMessages = new List<Message>();
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var decoder = new DefaultDecoder();
                var topicCount = new Dictionary<string, int> { { topic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount, decoder);

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

            Assert.AreEqual(2, resultMessages.Count);
            Assert.AreEqual(msg1.ToString(), resultMessages[0].ToString());
            Assert.AreEqual(msg2.ToString(), resultMessages[1].ToString());
        }

        [Test]
        public void SimpleSyncProducerSendsLotsOfMessagesAndConsumerConnectorGetsThemBack()
        {
            var prodConfig = this.SyncProducerConfig1;
            var consumerConfig = this.ZooKeeperBasedConsumerConfig;
            int numberOfMessages = 500;
            int messageSize = 0;

            List<Message> messagesToSend = new List<Message>();

            var producerHelper = new ProducerHelper();

            for (int i = 0; i < numberOfMessages; i++)
            {
                string payload1 = "kafka 0.8";
                byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
                var msg = new Message(payloadData1);
                messagesToSend.Add(msg);
                if (i == 0)
                {
                    messageSize = msg.Size;
                }
                producerHelper.SendMessagesToTopicSynchronously(CurrentTestTopic, new List<Message>() { msg }, prodConfig);
            }

            Thread.Sleep(2000);

            // now consuming
            int resultCount = 0;
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var decoder = new DefaultDecoder();
                var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount, decoder);
                var sets = messages[CurrentTestTopic];

                try
                {
                    foreach (var set in sets)
                    {
                        foreach (var message in set)
                        {
                            Assert.AreEqual(messageSize, message.Size);
                            resultCount++;
                        }
                    }
                }
                catch (ConsumerTimeoutException)
                {
                    // do nothing, this is expected
                }
            }

            Assert.AreEqual(numberOfMessages, resultCount);
        }

        [Test]
        public void OneMessageIsSentAndReceivedThenExceptionsWhenNoMessageThenAnotherMessageIsSentAndReceived()
        {
            var prodConfig = this.SyncProducerConfig1;
            var consumerConfig = this.ZooKeeperBasedConsumerConfig;
            var decoder = new DefaultDecoder();
            // first producing
            string payload1 = "kafka 1.";
            byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
            var msg1 = new Message(payloadData1);

            var producerHelper = new ProducerHelper();
            producerHelper.SendMessagesToTopicSynchronously(CurrentTestTopic, new List<Message>() { msg1 }, prodConfig);


            // now consuming
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount, decoder);
                var sets = messages[CurrentTestTopic];
                KafkaMessageStream<Message> myStream = sets[0];
                var enumerator = myStream.GetEnumerator();

                Assert.IsTrue(enumerator.MoveNext());
                Assert.AreEqual(msg1.ToString(), enumerator.Current.ToString());

                Assert.Throws<ConsumerTimeoutException>(() => enumerator.MoveNext());

                enumerator.Reset();

                // producing again
                string payload2 = "kafka 2.";
                byte[] payloadData2 = Encoding.UTF8.GetBytes(payload2);
                var msg2 = new Message(payloadData2);

                producerHelper.SendMessagesToTopicSynchronously(CurrentTestTopic, new List<Message>() { msg2 }, prodConfig);
                Thread.Sleep(3000);

                Assert.IsTrue(enumerator.MoveNext());
                Assert.AreEqual(msg2.ToString(), enumerator.Current.ToString());
            }
        }

        [Test]
        public void ConsumerConnectorConsumesTwoDifferentTopics()
        {
            var prodConfig = this.SyncProducerConfig1;
            var consumerConfig = this.ZooKeeperBasedConsumerConfig;

            string topic1 = CurrentTestTopic + "1";
            string topic2 = CurrentTestTopic + "2";

            // first producing
            string payload1 = "kafka 1.";
            byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
            var msg1 = new Message(payloadData1);

            string payload2 = "kafka 2.";
            byte[] payloadData2 = Encoding.UTF8.GetBytes(payload2);
            var msg2 = new Message(payloadData2);

            var producerHelper = new ProducerHelper();
            producerHelper.SendMessagesToTopicSynchronously(topic1, new List<Message>() { msg1 }, prodConfig);
            producerHelper.SendMessagesToTopicSynchronously(topic2, new List<Message>() { msg2 }, prodConfig);

            // now consuming
            var resultMessages1 = new List<Message>();
            var resultMessages2 = new List<Message>();
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var decoder = new DefaultDecoder();
                var topicCount = new Dictionary<string, int> { { topic1, 1 }, { topic2, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount, decoder);

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

            Assert.AreEqual(1, resultMessages1.Count);
            Assert.AreEqual(msg1.ToString(), resultMessages1[0].ToString());

            Assert.AreEqual(1, resultMessages2.Count);
            Assert.AreEqual(msg2.ToString(), resultMessages2[0].ToString());
        }

        [Test]
        public void ConsumerConnectorReceivesAShutdownSignal()
        {
            var consumerConfig = this.ZooKeeperBasedConsumerConfig;

            // now consuming
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var decoder = new DefaultDecoder();
                var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount, decoder);

                // putting the shutdown command into the queue
                FieldInfo fi = typeof(ZookeeperConsumerConnector).GetField(
                    "queues", BindingFlags.NonPublic | BindingFlags.Instance);
                var value =
                    (IDictionary<Tuple<string, string>, BlockingCollection<FetchedDataChunk>>)
                    fi.GetValue(consumerConnector);
                foreach (var topicConsumerQueueMap in value)
                {
                    topicConsumerQueueMap.Value.Add(ZookeeperConsumerConnector.ShutdownCommand);
                }

                var sets = messages[CurrentTestTopic];
                var resultMessages = new List<Message>();

                foreach (var set in sets)
                {
                    foreach (var message in set)
                    {
                        resultMessages.Add(message);
                    }
                }

                Assert.AreEqual(0, resultMessages.Count);
            }
        }

        [Test]
        public void SimpleSyncProducerSendsLotsOfMessagesIncreasingTheSizeAndConsumerConnectorGetsThemBack()
        {
            var prodConfig = this.SyncProducerConfig1;
            var consumerConfig = this.ZooKeeperBasedConsumerConfig;
            var consConf = this.ConsumerConfig1;
            consumerConfig.AutoCommitInterval = 1000;
            int numberOfMessagesToSend = 2000;
            string topic = CurrentTestTopic;

            var msgList = new List<Message>();

            var producerHelper = new ProducerHelper();

            for (int i = 0; i < numberOfMessagesToSend; i++)
            {
                string payload = CreatePayloadByNumber(i);
                byte[] payloadData = Encoding.UTF8.GetBytes(payload);
                var msg = new Message(payloadData);
                msgList.Add(msg);
                producerHelper.SendMessagesToTopicSynchronously(topic, new List<Message>() { msg }, prodConfig);
            }

            // now consuming
            int messageNumberCounter = 0;
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var decoder = new DefaultDecoder();
                var topicCount = new Dictionary<string, int> { { topic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount, decoder);
                var sets = messages[topic];

                try
                {
                    foreach (var set in sets)
                    {
                        foreach (var message in set)
                        {
                            Assert.AreEqual(CreatePayloadByNumber(messageNumberCounter), Encoding.UTF8.GetString(message.Payload));
                            messageNumberCounter++;
                        }
                    }
                }
                catch (ConsumerTimeoutException)
                {
                    // do nothing, this is expected
                }
            }

            Assert.AreEqual(numberOfMessagesToSend, messageNumberCounter);
        }

        private string CreatePayloadByNumber(int number)
        {
            return "kafkaTest " + new string('a', number);
        }

        [Test]
        public void SimpleSyncProducerSendsLotsOfCompressedMessagesAndConsumerConnectorGetsThemBack()
        {
            var prodConfig = this.SyncProducerConfig1;
            var consumerConfig = this.ZooKeeperBasedConsumerConfig;
            int numberOfMessages = 500;
            int messagesPerPackage = 5;
            int messageSize = 0;

            var producerHelper = new ProducerHelper();

            for (int i = 0; i < numberOfMessages; i++)
            {
                var messagePackageList = new List<Message>();
                for (int messageInPackageNr = 0; messageInPackageNr < messagesPerPackage; messageInPackageNr++)
                {
                    string payload1 = "kafka 1.";
                    byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
                    var msg = new Message(payloadData1);
                    messagePackageList.Add(msg);
                    if (i == 0 && messageInPackageNr == 0)
                    {
                        messageSize = msg.Size;
                    }
                }
                var packageMessage = CompressionUtils.Compress(messagePackageList, CompressionCodecs.GZIPCompressionCodec);

                producerHelper.SendMessagesToTopicSynchronously(CurrentTestTopic, new List<Message>() { packageMessage }, prodConfig);
            }

            Thread.Sleep(2000);

            // now consuming
            int resultCount = 0;
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var decoder = new DefaultDecoder();
                var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount, decoder);
                var sets = messages[CurrentTestTopic];

                try
                {
                    foreach (var set in sets)
                    {
                        foreach (var message in set)
                        {
                            Assert.AreEqual(messageSize, message.Size);
                            resultCount++;
                        }
                    }
                }
                catch (ConsumerTimeoutException)
                {
                    // do nothing, this is expected
                }
            }

            Assert.AreEqual(numberOfMessages * messagesPerPackage, resultCount);
        }

        [Test]
        public void SimpleSyncProducerSendsLotsOfCompressedMessagesAndConsumerConnectorGetsThemBackWithAbreakInTheMiddle()
        {
            var prodConfig = this.SyncProducerConfig1;
            var consumerConfig = this.ZooKeeperBasedConsumerConfig;
            int numberOfMessages = 500;
            int messagesPerPackage = 5;
            int messageSize = 0;
            int breakAtMessageNr = 1573;
            int partOfPackedMessagesThatWillBeConsumedTwice = breakAtMessageNr % messagesPerPackage;

            var producerHelper = new ProducerHelper();
            for (int i = 0; i < numberOfMessages; i++)
            {
                var messagePackageList = new List<Message>();
                for (int messageInPackageNr = 0; messageInPackageNr < messagesPerPackage; messageInPackageNr++)
                {
                    string payload1 = "kafka 1.";
                    byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
                    var msg = new Message(payloadData1);
                    messagePackageList.Add(msg);
                    if (i == 0 && messageInPackageNr == 0)
                    {
                        messageSize = msg.Size;
                    }
                }
                var packageMessage = CompressionUtils.Compress(messagePackageList, CompressionCodecs.GZIPCompressionCodec);
                producerHelper.SendMessagesToTopicSynchronously(CurrentTestTopic, new List<Message>() { packageMessage }, prodConfig);
            }

            Thread.Sleep(2000);

            // now consuming
            int resultCount = 0;
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var decoder = new DefaultDecoder();
                var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount, decoder);
                var sets = messages[CurrentTestTopic];

                try
                {
                    foreach (var set in sets)
                    {
                        foreach (var message in set)
                        {
                            Assert.AreEqual(messageSize, message.Size);
                            resultCount++;
                            if (resultCount == breakAtMessageNr)
                            {
                                break;
                            }
                        }
                        if (resultCount == breakAtMessageNr)
                        {
                            break;
                        }
                    }
                }
                catch (ConsumerTimeoutException)
                {
                    // do nothing, this is expected
                }
            }

            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var decoder = new DefaultDecoder();
                var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount, decoder);
                var sets = messages[CurrentTestTopic];

                try
                {
                    foreach (var set in sets)
                    {
                        foreach (var message in set)
                        {
                            Assert.AreEqual(messageSize, message.Size);
                            resultCount++;
                        }
                    }
                }
                catch (ConsumerTimeoutException)
                {
                    // do nothing, this is expected
                }
            }

            Assert.AreEqual(numberOfMessages * messagesPerPackage + partOfPackedMessagesThatWillBeConsumedTwice, resultCount);
        }

        [Test]
        public void SimpleSyncProducerSendsLotsOfTwiceCompressedMessagesAndConsumerConnectorGetsThemBack()
        {
            var prodConfig = this.SyncProducerConfig1;
            var consumerConfig = this.ZooKeeperBasedConsumerConfig;
            int numberOfMessages = 500;
            int messagesPerPackage = 5;
            int messageSize = 0;
            int messagesPerInnerPackage = 5;

            var producerHelper = new ProducerHelper();
            for (int i = 0; i < numberOfMessages; i++)
            {
                var messagePackageList = new List<Message>();
                for (int messageInPackageNr = 0; messageInPackageNr < messagesPerPackage; messageInPackageNr++)
                {
                    var innerMessagePackageList = new List<Message>();
                    for (int inner = 0; inner < messagesPerInnerPackage; inner++)
                    {
                        string payload1 = "kafka 1.";
                        byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
                        var msg = new Message(payloadData1);
                        innerMessagePackageList.Add(msg);
                    }
                    var innerPackageMessage = CompressionUtils.Compress(innerMessagePackageList, CompressionCodecs.GZIPCompressionCodec);
                    messagePackageList.Add(innerPackageMessage);
                }
                var packageMessage = CompressionUtils.Compress(messagePackageList, CompressionCodecs.GZIPCompressionCodec);

                producerHelper.SendMessagesToTopicSynchronously(CurrentTestTopic, new List<Message>() { packageMessage }, prodConfig);
            }

            Thread.Sleep(2000);

            // now consuming
            int resultCount = 0;
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var decoder = new DefaultDecoder();
                var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount, decoder);
                var sets = messages[CurrentTestTopic];

                try
                {
                    foreach (var set in sets)
                    {
                        foreach (var message in set)
                        {
                            resultCount++;
                        }
                    }
                }
                catch (ConsumerTimeoutException)
                {
                    // do nothing, this is expected
                }
            }

            Assert.AreEqual(numberOfMessages * messagesPerPackage * messagesPerInnerPackage, resultCount);
        }

        [Test]
        public void SimpleSyncProducerSendsLotsOfCompressedMessagesWithIncreasedSizeAndConsumerConnectorGetsThemBack()
        {
            var prodConfig = this.SyncProducerConfig1;
            var consumerConfig = this.ZooKeeperBasedConsumerConfig;
            consumerConfig.AutoCommit = true;
            consumerConfig.AutoCommitInterval = 100;
            int numberOfMessages = 2000;
            int messagesPerPackage = 5;
            string topic = CurrentTestTopic;

            int msgNr = 0;
            long totalSize = 0;

            var producerHelper = new ProducerHelper();
            for (int i = 0; i < numberOfMessages; i++)
            {
                var messagePackageList = new List<Message>();
                for (int messageInPackageNr = 0; messageInPackageNr < messagesPerPackage; messageInPackageNr++)
                {
                    string payload1 = CreatePayloadByNumber(msgNr);
                    byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
                    var msg = new Message(payloadData1);
                    totalSize += msg.Size;
                    messagePackageList.Add(msg);
                    msgNr++;
                }
                var packageMessage = CompressionUtils.Compress(messagePackageList, CompressionCodecs.GZIPCompressionCodec);
                producerHelper.SendMessagesToTopicSynchronously(topic, new List<Message>() { packageMessage }, prodConfig);
            }

            // now consuming
            int resultCount = 0;
            long resultSize = 0;
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var decoder = new DefaultDecoder();
                var topicCount = new Dictionary<string, int> { { topic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount, decoder);
                var sets = messages[topic];

                try
                {
                    foreach (var set in sets)
                    {
                        foreach (var message in set)
                        {
                            Assert.AreEqual(CreatePayloadByNumber(resultCount), Encoding.UTF8.GetString(message.Payload));
                            resultCount++;
                            resultSize += message.Size;
                        }
                    }
                }
                catch (ConsumerTimeoutException)
                {
                    // do nothing, this is expected
                }
            }

            Assert.AreEqual(numberOfMessages * messagesPerPackage, resultCount);
            Assert.AreEqual(totalSize, resultSize);
        }

        [Test]
        public void SimpleSyncProducerSendsLotsOfMessagesAndConsumerConnectorGetsThemBackWithVerySmallAutoCommitInterval()
        {
            var prodConfig = this.SyncProducerConfig1;
            var consumerConfig = this.ZooKeeperBasedConsumerConfig;
            consumerConfig.AutoCommit = true;
            consumerConfig.AutoCommitInterval = 10;
            int numberOfMessages = 500;
            int messageSize = 0;

            List<Message> messagesToSend = new List<Message>();

            var producerHelper = new ProducerHelper();
            for (int i = 0; i < numberOfMessages; i++)
            {
                string payload1 = "kafka 1.";
                byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
                var msg = new Message(payloadData1);
                messagesToSend.Add(msg);
                if (i == 0)
                {
                    messageSize = msg.Size;
                }
                producerHelper.SendMessagesToTopicSynchronously(CurrentTestTopic, new List<Message>() { msg }, prodConfig);
            }

            Thread.Sleep(2000);

            // now consuming
            int resultCount = 0;
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var decoder = new DefaultDecoder();
                var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount, decoder);
                var sets = messages[CurrentTestTopic];

                try
                {
                    foreach (var set in sets)
                    {
                        foreach (var message in set)
                        {
                            Assert.AreEqual(messageSize, message.Size);
                            resultCount++;
                        }
                    }
                }
                catch (ConsumerTimeoutException)
                {
                    // do nothing, this is expected
                }
            }

            Assert.AreEqual(numberOfMessages, resultCount);
        }

        [Test]
        public void MaxFetchSizeBugShouldNotAppearWhenSmallFetchSizeAndSingleMessageSmallerThanFetchSize()
        {
            var prodConfig = this.SyncProducerConfig1;
            var consumerConfig = this.ZooKeeperBasedConsumerConfig;
            consumerConfig.FetchSize = 256;
            consumerConfig.NumberOfTries = 1;
            consumerConfig.AutoCommitInterval = 1000;
            int numberOfMessagesToSend = 100;
            string topic = CurrentTestTopic;

            Assert.True(consumerConfig.FetchSize >= numberOfMessagesToSend + 16);

            var msgList = new List<Message>();
            var producerHelper = new ProducerHelper();
            for (int i = 0; i < numberOfMessagesToSend; i++)
            {
                string payload = CreatePayloadByNumber(i);
                byte[] payloadData = Encoding.UTF8.GetBytes(payload);
                var msg = new Message(payloadData);
                msgList.Add(msg);
                producerHelper.SendMessagesToTopicSynchronously(topic, new List<Message>() { msg }, prodConfig);
            }

            // now consuming
            int messageNumberCounter = 0;
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var decoder = new DefaultDecoder();
                var topicCount = new Dictionary<string, int> { { topic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount, decoder);
                var sets = messages[topic];


                foreach (var set in sets)
                {
                    try
                    {
                        foreach (var message in set)
                        {
                            Assert.AreEqual(CreatePayloadByNumber(messageNumberCounter), Encoding.UTF8.GetString(message.Payload));
                            messageNumberCounter++;
                        }
                    }
                    catch (ConsumerTimeoutException)
                    {
                        // do nothing, this is expected
                    }
                }

            }

            Assert.AreEqual(numberOfMessagesToSend, messageNumberCounter);
        }

        [Test]
        public void InvalidMessageSizeShouldAppearWhenMessageIsLargerThanTheFetchSize()
        {
            var prodConfig = this.SyncProducerConfig1;
            var consumerConfig = this.ZooKeeperBasedConsumerConfig;
            consumerConfig.FetchSize = 256;
            consumerConfig.NumberOfTries = 1;
            consumerConfig.AutoCommitInterval = 1000;
            int numberOfMessagesToSend = 1;
            string topic = CurrentTestTopic;

            var msgList = new List<Message>();
            var producerHelper = new ProducerHelper();
            for (int i = 0; i < numberOfMessagesToSend; i++)
            {
                string payload = CreatePayloadByNumber(i + 10000);
                byte[] payloadData = Encoding.UTF8.GetBytes(payload);
                var msg = new Message(payloadData);
                msgList.Add(msg);
                producerHelper.SendMessagesToTopicSynchronously(topic, new List<Message>() { msg }, prodConfig);
            }

            // now consuming
            int messageNumberCounter = 0;
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var decoder = new DefaultDecoder();
                var topicCount = new Dictionary<string, int> { { topic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount, decoder);
                var sets = messages[topic];

                try
                {
                    foreach (var set in sets)
                    {
                        foreach (var message in set)
                        {
                            Assert.AreEqual(CreatePayloadByNumber(messageNumberCounter + 100), Encoding.UTF8.GetString(message.Payload));
                            messageNumberCounter++;
                        }
                    }
                }
                catch (ConsumerTimeoutException)
                {
                    // do nothing, this is expected
                }
            }

            Assert.AreEqual(0, messageNumberCounter);
        }
    }
}
