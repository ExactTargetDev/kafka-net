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

using Kafka.Client.Responses;

namespace Kafka.Client.IntegrationTests
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using Kafka.Client.Cfg;
    using Kafka.Client.Consumers;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Requests;
    using Kafka.Client.Serialization;
    using NUnit.Framework;
    using Kafka.Client.Producers.Sync;

    [TestFixture]
    public class ProducerTests : IntegrationFixtureBase
    {
        /// <summary>
        /// Maximum amount of time to wait trying to get a specific test message from Kafka server (in miliseconds)
        /// </summary>
        private readonly int maxTestWaitTimeInMiliseconds = 5000;

        [Test]
        public void SyncProducerSendsTopicMetadataRequestWithOneTopic()
        {
            var prodConfig = this.SyncProducerConfig1;
            int waitSingle = 100;
            int totalWaitTimeInMiliseconds = 0;

            var topic = CurrentTestTopic;

            TopicMetadataRequest request = new TopicMetadataRequest(new List<string>() { topic });

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

            TopicMetadataRequest request = new TopicMetadataRequest(new List<string>() { topic1, topic2 });

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

            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(new List<string>() { topic });
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
                var req = new ProducerRequest(-1, "", 0, 0,
                                              new List<TopicData>()
                                                  {
                                                      new TopicData(CurrentTestTopic,
                                                                    new List<PartitionData>()
                                                                        {
                                                                            new PartitionData(
                                                                                partitionMetadata.PartitionId,
                                                                                bufferedMessageSet)
                                                                        })
                                                  });
                var producerResponse = producer.Send(req);
                Assert.NotNull(producerResponse);
                Assert.AreEqual(1, producerResponse.Offsets.Count());
                Assert.Greater(producerResponse.Offsets.ToArray()[0], 0);
            }
        }

        [Test]
        public void ProducerSends3Messages()
        {
            var prodConfig = this.ConfigBasedSyncProdConfig;

            int totalWaitTimeInMiliseconds = 0;
            int waitSingle = 100;
            var originalMessage1 = new Message(Encoding.UTF8.GetBytes("TestData1"));
            var originalMessage2 = new Message(Encoding.UTF8.GetBytes("TestData2"));
            var originalMessage3 = new Message(Encoding.UTF8.GetBytes("TestData3"));
            var originalMessageList = new List<Message> { originalMessage1, originalMessage2, originalMessage3 };

            var multipleBrokersHelper = new TestMultipleBrokersHelper(CurrentTestTopic);
            multipleBrokersHelper.GetCurrentOffsets(
                new[] { this.SyncProducerConfig1, this.SyncProducerConfig2, this.SyncProducerConfig3 });
            using (var producer = new Producer(prodConfig))
            {
                var producerData = new ProducerData<string, Message>(CurrentTestTopic, originalMessageList);
                producer.Send(producerData);
            }

            Thread.Sleep(waitSingle);
            while (
                !multipleBrokersHelper.CheckIfAnyBrokerHasChanged(
                    new[] { this.SyncProducerConfig1, this.SyncProducerConfig2, this.SyncProducerConfig3 }))
            {
                totalWaitTimeInMiliseconds += waitSingle;
                Thread.Sleep(waitSingle);
                if (totalWaitTimeInMiliseconds > this.maxTestWaitTimeInMiliseconds)
                {
                    Assert.Fail("None of the brokers changed their offset after sending a message");
                }
            }

            totalWaitTimeInMiliseconds = 0;

            var consumerConfig = new ConsumerConfiguration(
                multipleBrokersHelper.BrokerThatHasChanged.Host, multipleBrokersHelper.BrokerThatHasChanged.Port);
            IConsumer consumer = new Consumer(consumerConfig);
            var request = new FetchRequest(CurrentTestTopic, multipleBrokersHelper.PartitionThatHasChanged, multipleBrokersHelper.OffsetFromBeforeTheChange);

            BufferedMessageSet response;
            while (true)
            {
                Thread.Sleep(waitSingle);
                response = consumer.Fetch(request);
                if (response != null && response.Messages.Count() > 2)
                {
                    break;
                }

                totalWaitTimeInMiliseconds += waitSingle;
                if (totalWaitTimeInMiliseconds >= this.maxTestWaitTimeInMiliseconds)
                {
                    break;
                }
            }

            Assert.NotNull(response);
            Assert.AreEqual(3, response.Messages.Count());
            Assert.AreEqual(originalMessage1.ToString(), response.Messages.First().ToString());
            Assert.AreEqual(originalMessage2.ToString(), response.Messages.Skip(1).First().ToString());
            Assert.AreEqual(originalMessage3.ToString(), response.Messages.Skip(2).First().ToString());
        }

        [Test]
        public void ProducerSends1MessageUsingNotDefaultEncoder()
        {
            var prodConfig = this.ConfigBasedSyncProdConfig;

            int totalWaitTimeInMiliseconds = 0;
            int waitSingle = 100;
            string originalMessage = "TestData";

            var multipleBrokersHelper = new TestMultipleBrokersHelper(CurrentTestTopic);
            multipleBrokersHelper.GetCurrentOffsets(new[] { this.SyncProducerConfig1, this.SyncProducerConfig2, this.SyncProducerConfig3 });
            using (var producer = new Producer<string, string>(prodConfig, null, new StringEncoder(), null))
            {
                var producerData = new ProducerData<string, string>(
                    CurrentTestTopic, new List<string> { originalMessage });

                producer.Send(producerData);
            }

            Thread.Sleep(waitSingle);

            while (!multipleBrokersHelper.CheckIfAnyBrokerHasChanged(new[] { this.SyncProducerConfig1, this.SyncProducerConfig2, this.SyncProducerConfig3 }))
            {
                totalWaitTimeInMiliseconds += waitSingle;
                Thread.Sleep(waitSingle);
                if (totalWaitTimeInMiliseconds > this.maxTestWaitTimeInMiliseconds)
                {
                    Assert.Fail("None of the brokers changed their offset after sending a message");
                }
            }

            totalWaitTimeInMiliseconds = 0;

            var consumerConfig = new ConsumerConfiguration(
                multipleBrokersHelper.BrokerThatHasChanged.Host,
                    multipleBrokersHelper.BrokerThatHasChanged.Port);
            IConsumer consumer = new Consumer(consumerConfig);
            var request = new FetchRequest(CurrentTestTopic, multipleBrokersHelper.PartitionThatHasChanged, multipleBrokersHelper.OffsetFromBeforeTheChange);

            BufferedMessageSet response;
            while (true)
            {
                Thread.Sleep(waitSingle);
                response = consumer.Fetch(request);
                if (response != null && response.Messages.Count() > 0)
                {
                    break;
                }

                totalWaitTimeInMiliseconds += waitSingle;
                if (totalWaitTimeInMiliseconds >= this.maxTestWaitTimeInMiliseconds)
                {
                    break;
                }
            }

            Assert.NotNull(response);
            Assert.AreEqual(1, response.Messages.Count());
            Assert.AreEqual(originalMessage, Encoding.UTF8.GetString(response.Messages.First().Payload));
        }
    }
}
