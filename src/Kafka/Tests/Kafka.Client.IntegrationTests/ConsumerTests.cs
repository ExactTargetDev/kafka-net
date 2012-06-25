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
    using Kafka.Client.Cfg;

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
            IConsumer consumer = new Consumer(config, "192.168.1.39", 9092);
            IList<long> list = consumer.GetOffsetsBefore(request);
            int a = 5;
        }

        [Test]
        public void SimpleSyncProducerSends1MessageAndConsumerGetsItBack()
        {
            ProducerHelper ph = new ProducerHelper();
            var topic = CurrentTestTopic;
            var prodConfig = this.SyncProducerConfig1;
            string payload1 = "TestData.";
            byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
            var msg1 = new Message(payloadData1);
            var producerResult = ph.SendMessagesToTopicSynchronously(topic, new List<Message>() { msg1 }, prodConfig);
            var producerResponse = producerResult.Item1;
            var partitionMetadata = producerResult.Item2;

            var broker = partitionMetadata.Replicas.ToArray()[0];

            var consumerConfig = this.ConsumerConfig1;
            consumerConfig.Broker.BrokerId = broker.Id;
            consumerConfig.Broker.Host = broker.Host;
            consumerConfig.Broker.Port = broker.Port;
 
            var consumer = new Consumer(consumerConfig);
            var fetchRequest = new FetchRequest(-1, "", broker.Id, -1, -1, new List<OffsetDetail>() { new OffsetDetail(topic, new List<int>() {partitionMetadata.PartitionId}, new List<long>() { 0 }, new List<int>() { 256 }) });

            // giving the kafka server some time to process the message
            Thread.Sleep(5000);

            var fetchResponse = consumer.Fetch(fetchRequest);

            var messageSet = fetchResponse.MessageSet(topic, partitionMetadata.PartitionId);
            var messages = messageSet.ToList();
            Assert.AreEqual(1, messages.Count);
            Assert.AreEqual(payloadData1, messages[0].Message.Payload);
        }

        [Test]
        public void SimpleSyncProducerSends2MessagesAndConsumerGetsThemBack()
        {
            ProducerHelper ph = new ProducerHelper();
            var topic = CurrentTestTopic;
            var prodConfig = this.SyncProducerConfig1;
            string payload1 = "TestData.";
            byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
            var msg1 = new Message(payloadData1);
            string payload2 = "TestData2.";
            byte[] payloadData2 = Encoding.UTF8.GetBytes(payload2);
            var msg2 = new Message(payloadData2);
            var producerResult = ph.SendMessagesToTopicSynchronously(topic, new List<Message>() { msg1, msg2 }, prodConfig);
            var producerResponse = producerResult.Item1;
            var partitionMetadata = producerResult.Item2;

            var broker = partitionMetadata.Replicas.ToArray()[0];

            var consumerConfig = this.ConsumerConfig1;
            consumerConfig.Broker.BrokerId = broker.Id;
            consumerConfig.Broker.Host = broker.Host;
            consumerConfig.Broker.Port = broker.Port;

            var consumer = new Consumer(consumerConfig);
            var fetchRequest = new FetchRequest(-1, "", broker.Id, -1, -1, new List<OffsetDetail>() { new OffsetDetail(topic, new List<int>() {partitionMetadata.PartitionId}, new List<long>() { 0 }, new List<int>() { 256 }) });

            // giving the kafka server some time to process the message
            Thread.Sleep(10000);

            var fetchResponse = consumer.Fetch(fetchRequest);

            var messageSet = fetchResponse.MessageSet(topic, partitionMetadata.PartitionId);
            var messages = messageSet.ToList();
            Assert.AreEqual(2, messages.Count);
            Assert.AreEqual(payloadData1, messages[0].Message.Payload);
            Assert.AreEqual(payloadData2, messages[1].Message.Payload);
        }

        //[Test]
        //public void SimpleSyncProducerSends2MessagesAndConsumerConnectorGetsThemBack()
        //{
        //    var prodConfig = this.SyncProducerConfig1;
        //    var consumerConfig = this.ZooKeeperBasedConsumerConfig;
        //    var consConf = this.ConsumerConfig1;

        //    // first producing
        //    string payload1 = "kafka 1.";
        //    byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
        //    var msg1 = new Message(payloadData1);

        //    string payload2 = "kafka 2.";
        //    byte[] payloadData2 = Encoding.UTF8.GetBytes(payload2);
        //    var msg2 = new Message(payloadData2);

        //    var producerRequest = new ProducerRequest(CurrentTestTopic, 0, new List<Message> { msg1, msg2 });
        //    using (var producer = new SyncProducer(prodConfig))
        //    {
        //        producer.Send(producerRequest);
        //    }

        //    var consumer = new Consumer(consConf);
        //    long offset = 0;
        //    var result = consumer.Fetch(
        //        new FetchRequest(CurrentTestTopic, 0, offset, 400));
        //    foreach (var resultItem in result)
        //    {
        //        offset += resultItem.Offset;
        //    }

        //    // now consuming
        //    var resultMessages = new List<Message>();
        //    using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
        //    {
        //        var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
        //        var messages = consumerConnector.CreateMessageStreams(topicCount);
        //        var sets = messages[CurrentTestTopic];
        //        try
        //        {
        //            foreach (var set in sets)
        //            {
        //                foreach (var message in set)
        //                {
        //                    resultMessages.Add(message);
        //                }
        //            }
        //        }
        //        catch (ConsumerTimeoutException)
        //        {
        //            // do nothing, this is expected
        //        }
        //    }

        //    Assert.AreEqual(2, resultMessages.Count);
        //    Assert.AreEqual(msg1.ToString(), resultMessages[0].ToString());
        //    Assert.AreEqual(msg2.ToString(), resultMessages[1].ToString());
        //}

        [Test]
        public void SimpleSyncProducerSendsLotsOfMessagesAndConsumerConnectorGetsThemBack()
        {
            var prodConfig = this.SyncProducerConfig1;
            var consumerConfig = this.ZooKeeperBasedConsumerConfig;
            var consConf = this.ConsumerConfig1;
            int numberOfMessages = 500;
            int messageSize = 0;

            List<Message> messagesToSend = new List<Message>();

            using (var producer = new SyncProducer(prodConfig))
            {
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
                    producer.Send(CurrentTestTopic, 0, new List<Message>() { msg });
                }
            }

            Thread.Sleep(2000);

            // now consuming
            int resultCount = 0;
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount);
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

            // first producing
            string payload1 = "kafka 1.";
            byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
            var msg1 = new Message(payloadData1);
            using (var producer = new SyncProducer(prodConfig))
            {
                var producerRequest = new ProducerRequest(CurrentTestTopic, 0, new List<Message> { msg1 });
                producer.Send(producerRequest);

                // now consuming
                using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
                {
                    var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                    var messages = consumerConnector.CreateMessageStreams(topicCount);
                    var sets = messages[CurrentTestTopic];
                    KafkaMessageStream myStream = sets[0];
                    var enumerator = myStream.GetEnumerator();

                    Assert.IsTrue(enumerator.MoveNext());
                    Assert.AreEqual(msg1.ToString(), enumerator.Current.ToString());

                    Assert.Throws<ConsumerTimeoutException>(() => enumerator.MoveNext());

                    enumerator.Reset();

                    // producing again
                    string payload2 = "kafka 2.";
                    byte[] payloadData2 = Encoding.UTF8.GetBytes(payload2);
                    var msg2 = new Message(payloadData2);

                    var producerRequest2 = new ProducerRequest(CurrentTestTopic, 0, new List<Message> { msg2 });
                    producer.Send(producerRequest2);
                    Thread.Sleep(3000);

                    Assert.IsTrue(enumerator.MoveNext());
                    Assert.AreEqual(msg2.ToString(), enumerator.Current.ToString());
                }
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

            using (var producer = new SyncProducer(prodConfig))
            {
                var producerRequest1 = new ProducerRequest(topic1, 0, new List<Message> { msg1 });
                producer.Send(producerRequest1);
                var producerRequest2 = new ProducerRequest(topic2, 0, new List<Message> { msg2 });
                producer.Send(producerRequest2);
            }

            // now consuming
            var resultMessages1 = new List<Message>();
            var resultMessages2 = new List<Message>();
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var topicCount = new Dictionary<string, int> { { topic1, 1 }, { topic2, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount);

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
                var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount);

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
        public void ProducersSendMessagesToDifferentPartitionsAndConsumerConnectorGetsThemBack()
        {
            var prodConfig = this.SyncProducerConfig1;
            var consumerConfig = this.ZooKeeperBasedConsumerConfig;

            // first producing
            string payload1 = "kafka 1.";
            byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
            var msg1 = new Message(payloadData1);

            string payload2 = "kafka 2.";
            byte[] payloadData2 = Encoding.UTF8.GetBytes(payload2);
            var msg2 = new Message(payloadData2);

            using (var producer = new SyncProducer(prodConfig))
            {
                var producerRequest1 = new ProducerRequest(CurrentTestTopic, 0, new List<Message> { msg1 });
                producer.Send(producerRequest1);
                var producerRequest2 = new ProducerRequest(CurrentTestTopic, 1, new List<Message> { msg2 });
                producer.Send(producerRequest2);
            }

            // now consuming
            var resultMessages = new List<Message>();
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount);
                var sets = messages[CurrentTestTopic];
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
        public void SimpleSyncProducerSendsLotsOfMessagesIncreasingTheSizeAndConsumerConnectorGetsThemBack()
        {
            var prodConfig = this.SyncProducerConfig1;
            var consumerConfig = this.ZooKeeperBasedConsumerConfig;
            var consConf = this.ConsumerConfig1;
            consumerConfig.AutoCommitInterval = 1000;
            int numberOfMessagesToSend = 2000;
            string topic = CurrentTestTopic;

            var msgList = new List<Message>();
            using (var producer = new SyncProducer(prodConfig))
            {
                for (int i = 0; i < numberOfMessagesToSend; i++)
                {
                    string payload = CreatePayloadByNumber(i);
                    byte[] payloadData = Encoding.UTF8.GetBytes(payload);
                    var msg = new Message(payloadData);
                    msgList.Add(msg);
                    var producerRequest = new ProducerRequest(topic, 0, new List<Message>() { msg });
                    producer.Send(producerRequest);
                }
            }

            // now consuming
            int messageNumberCounter = 0;
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var topicCount = new Dictionary<string, int> { { topic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount);
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

            using (var producer = new SyncProducer(prodConfig))
            {
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

                    producer.Send(CurrentTestTopic, 0, new List<Message>() { packageMessage });
                }
            }

            Thread.Sleep(2000);

            // now consuming
            int resultCount = 0;
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount);
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

            using (var producer = new SyncProducer(prodConfig))
            {
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

                    producer.Send(CurrentTestTopic, 0, new List<Message>() { packageMessage });
                }
            }

            Thread.Sleep(2000);

            // now consuming
            int resultCount = 0;
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount);
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
                var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount);
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

            using (var producer = new SyncProducer(prodConfig))
            {
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

                    producer.Send(CurrentTestTopic, 0, new List<Message>() { packageMessage });
                }
            }

            Thread.Sleep(2000);

            // now consuming
            int resultCount = 0;
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount);
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

            var multipleBrokersHelper = new TestMultipleBrokersHelper(CurrentTestTopic);
            multipleBrokersHelper.GetCurrentOffsets(
                new[] { prodConfig });

            int msgNr = 0;
            long totalSize = 0;
            using (var producer = new SyncProducer(prodConfig))
            {
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
                    producer.Send(topic, 0, new List<Message>() { packageMessage });
                }
            }

            // now consuming
            int resultCount = 0;
            long resultSize = 0;
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var topicCount = new Dictionary<string, int> { { topic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount);
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

            using (var producer = new SyncProducer(prodConfig))
            {
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
                    producer.Send(CurrentTestTopic, 0, new List<Message>() { msg });
                }
            }

            Thread.Sleep(2000);

            // now consuming
            int resultCount = 0;
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount);
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

            var msgList = new List<Message>();
            using (var producer = new SyncProducer(prodConfig))
            {
                for (int i = 0; i < numberOfMessagesToSend; i++)
                {
                    string payload = CreatePayloadByNumber(i + 100);
                    byte[] payloadData = Encoding.UTF8.GetBytes(payload);
                    var msg = new Message(payloadData);
                    msgList.Add(msg);
                    var producerRequest = new ProducerRequest(topic, 0, new List<Message>() { msg });
                    producer.Send(producerRequest);
                }
            }

            // now consuming
            int messageNumberCounter = 0;
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var topicCount = new Dictionary<string, int> { { topic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount);
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

            Assert.AreEqual(numberOfMessagesToSend, messageNumberCounter);
        }

        [Test]
        public void InvalidMessageSizeShouldAppearWhenMessageIsLArgerThanTheFetchSize()
        {
            var prodConfig = this.SyncProducerConfig1;
            var consumerConfig = this.ZooKeeperBasedConsumerConfig;
            consumerConfig.FetchSize = 256;
            consumerConfig.NumberOfTries = 1;
            consumerConfig.AutoCommitInterval = 1000;
            int numberOfMessagesToSend = 1;
            string topic = CurrentTestTopic;

            var msgList = new List<Message>();
            using (var producer = new SyncProducer(prodConfig))
            {
                for (int i = 0; i < numberOfMessagesToSend; i++)
                {
                    string payload = CreatePayloadByNumber(i + 10000);
                    byte[] payloadData = Encoding.UTF8.GetBytes(payload);
                    var msg = new Message(payloadData);
                    msgList.Add(msg);
                    var producerRequest = new ProducerRequest(topic, 0, new List<Message>() { msg });
                    producer.Send(producerRequest);
                }
            }

            // now consuming
            int messageNumberCounter = 0;
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(consumerConfig, true))
            {
                var topicCount = new Dictionary<string, int> { { topic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount);
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
