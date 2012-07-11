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

using Kafka.Client.Producers;
using Kafka.Client.Serialization;

namespace Kafka.Client.IntegrationTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using Kafka.Client.Consumers;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers.Async;
    using Kafka.Client.Producers.Sync;
    using Kafka.Client.Requests;
    using NUnit.Framework;
    using Kafka.Client.Exceptions;
    using System.ComponentModel;
    using System.IO;

    /// <summary>
    /// Contains tests that go all the way to Kafka and back.
    /// </summary>
    [TestFixture]
    public class KafkaIntegrationTest : IntegrationFixtureBase
    {
        /// <summary>
        /// Maximum amount of time to wait trying to get a specific test message from Kafka server (in miliseconds)
        /// </summary>
        private static readonly int MaxTestWaitTimeInMiliseconds = 5000;

        private static int WhenConsumerConsumesAndLaterOthersJoinAndRebalanceOccursThenMessagesShouldNotBeDuplicated_BackgorundThreadsResultCounter = 0;
        private static int WhenConsumerConsumesAndLaterOthersJoinAndRebalanceOccursThenMessagesShouldNotBeDuplicated_BackgorundThreadsDuplicatesCounter = 0;

        private static List<Message>
            WhenConsumerConsumesAndLaterOthersJoinAndRebalanceOccursThenMessagesShouldNotBeDuplicated_BackgorundThreadsReceivedMessages
                = new List<Message>();
        private static readonly object WhenConsumerConsumesAndLaterOthersJoinAndRebalanceOccursThenMessagesShouldNotBeDuplicated_BackgorundThreadsResultCounterLock = new object();
        private static int WhenConsumerConsumesAndLaterOthersJoinAndRebalanceOccursThenMessagesShouldNotBeDuplicated_BackgorundThreadsDoneNr = 0;

        /// <summary>
        /// Asynchronously sends many random messages to Kafka
        /// </summary>
        [Test]
        public void AsyncProducerSendsManyLongRandomMessagesAndConsumerConnectorGetsThemBack()
        {
            var prodConfig = this.ZooKeeperBasedAsyncProdConfig;
            prodConfig.EnqueueTimeoutMs = 1000;
            var consConfig = this.ZooKeeperBasedConsumerConfig;
            List<string> messages = GenerateRandomMessageStrings(50);

            using (var producer = new Producer<int, string>(prodConfig))
            {
                Thread.Sleep(2000);
                string messageString = "ZkAwareProducerSends1Message - test message";
                ProducerData<int, string> producerData = new ProducerData<int, string>(CurrentTestTopic, messages);
                producer.Send(producerData);


                Thread.Sleep(2000);

                using (var consumerConnector = new ZookeeperConsumerConnector(consConfig, true))
                {
                    var decoder = new DefaultDecoder();
                    var topicCount = new Dictionary<string, int> {{CurrentTestTopic, 1}};
                    var messageStreams = consumerConnector.CreateMessageStreams(topicCount, decoder);
                    var resultMessages = new List<Message>();
                    var sets = messageStreams[CurrentTestTopic];
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
                    Assert.AreEqual(messages.Count, resultMessages.Count);
                }
            }
        }

        private class TestCallbackHandler //: ICallbackHandler
        {
            public bool WasRun { get; private set; }

            public void Handle(RequestContext<ProducerRequest> context)
            {
                WasRun = true;
            }
        }

        /// <summary>
        /// Gets offsets from Kafka.
        /// </summary>
        [Test]
        public void ConsumerGetsOffsets()
        {
            var consumerConfig = this.ConsumerConfig1;

            var request = new OffsetRequest(CurrentTestTopic, 0, DateTime.Now.AddHours(-24).Ticks, 10);
            IConsumer consumer = new Consumer(consumerConfig);
            IList<long> list = consumer.GetOffsetsBefore(request);

            foreach (long l in list)
            {
                Console.Out.WriteLine(l);
            }
        }

        /// <summary>
        /// Gererates a random list of messages strings.
        /// </summary>
        /// <param name="numberOfMessages">The number of strings to generate.</param>
        /// <returns>A list of random strings.</returns>
        private static List<string> GenerateRandomMessageStrings(int numberOfMessages)
        {
            var messages = new List<string>();
            for (int ix = 0; ix < numberOfMessages; ix++)
            {
                messages.Add((GenerateRandomMessage(10000)));
            }

            return messages;
        }

        /// <summary>
        /// Gererates a randome list of text messages.
        /// </summary>
        /// <param name="numberOfMessages">The number of messages to generate.</param>
        /// <returns>A list of random text messages.</returns>
        private static List<Message> GenerateRandomTextMessages(int numberOfMessages)
        {
            var messages = new List<Message>();
            for (int ix = 0; ix < numberOfMessages; ix++)
            {
                ////messages.Add(new Message(GenerateRandomBytes(10000)));
                messages.Add(new Message(Encoding.UTF8.GetBytes(GenerateRandomMessage(10000))));
            }

            return messages;
        }

        /// <summary>
        /// Generate a random message text.
        /// </summary>
        /// <param name="length">Length of the message string.</param>
        /// <returns>Random message string.</returns>
        private static string GenerateRandomMessage(int length)
        {
            var builder = new StringBuilder();
            var random = new Random();
            for (int i = 0; i < length; i++)
            {
                char ch = Convert.ToChar(Convert.ToInt32(
                    Math.Floor((26 * random.NextDouble()) + 65)));
                builder.Append(ch);
            }

            return builder.ToString();
        }

        [Test]
        public void WhenConsumerConsumesAndLaterOthersJoinAndRebalanceOccursThenMessagesShouldNotBeDuplicated()
        {
            var prodConfig = this.ZooKeeperBasedSyncProdConfig;
            var consumerConfig = this.ZooKeeperBasedConsumerConfig;
            //consumerConfig.FetchSize = 256;

            int originalNrOfMessages = 1000;

            using (var producer = new Producer<int, string>(prodConfig))
            {
                List<string> messages = new List<string>();
                for (int i = 0; i < originalNrOfMessages; i++)
                {
                    messages.Add("test message" + i);

                }
                ProducerData<int, string> producerData = new ProducerData<int, string>(CurrentTestTopic, messages);
                producer.Send(producerData);
            }

            Thread.Sleep(10000);

            BackgroundWorker bw1 = new BackgroundWorker();
            bw1.WorkerSupportsCancellation = true;
            bw1.DoWork += new DoWorkEventHandler(WhenConsumerConsumesAndLaterOthersJoinAndRebalanceOccursThenMessagesShouldNotBeDuplicated_DoWork);
            int runBw1AfterNIterations = 50;

            BackgroundWorker bw2 = new BackgroundWorker();
            bw2.WorkerSupportsCancellation = true;
            bw2.DoWork += new DoWorkEventHandler(WhenConsumerConsumesAndLaterOthersJoinAndRebalanceOccursThenMessagesShouldNotBeDuplicated_DoWork);
            int runBw2AfterNIterations = 150;

            // now consuming
            int messageNumberCounter = 0;
            StringBuilder sb = new StringBuilder();
            var receivedMessages = new List<Message>();
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
                            receivedMessages.Add(message);
                            if (messageNumberCounter == runBw1AfterNIterations)
                            {
                                bw1.RunWorkerAsync();
                            }
                            if (messageNumberCounter == runBw2AfterNIterations)
                            {
                                bw2.RunWorkerAsync();
                            }
                            var msgString = Encoding.UTF8.GetString(message.Payload);
                            sb.AppendLine(msgString);
                            messageNumberCounter++;
                        }
                    }
                }
                catch (ConsumerTimeoutException)
                {
                    // do nothing, this is expected
                }
            }
            int finishedThreads = 0;
            var receivedFromBackgroundSoFar = new List<Message>();
            
            while (WhenConsumerConsumesAndLaterOthersJoinAndRebalanceOccursThenMessagesShouldNotBeDuplicated_BackgorundThreadsDoneNr < 2 && (messageNumberCounter + WhenConsumerConsumesAndLaterOthersJoinAndRebalanceOccursThenMessagesShouldNotBeDuplicated_BackgorundThreadsResultCounter < originalNrOfMessages))
            {
                lock (WhenConsumerConsumesAndLaterOthersJoinAndRebalanceOccursThenMessagesShouldNotBeDuplicated_BackgorundThreadsResultCounterLock)
                {
                    finishedThreads =
                        WhenConsumerConsumesAndLaterOthersJoinAndRebalanceOccursThenMessagesShouldNotBeDuplicated_BackgorundThreadsDoneNr;
                    receivedFromBackgroundSoFar.Clear();
                    receivedFromBackgroundSoFar.AddRange(WhenConsumerConsumesAndLaterOthersJoinAndRebalanceOccursThenMessagesShouldNotBeDuplicated_BackgorundThreadsReceivedMessages);
                }
                if (finishedThreads >= 2 || (receivedMessages.Count + receivedFromBackgroundSoFar.Count) >= originalNrOfMessages)
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            using (StreamWriter outfile = new StreamWriter("ConsumerTestDumpMain.txt"))
            {
                outfile.Write(sb.ToString());
            }
            receivedMessages.AddRange(WhenConsumerConsumesAndLaterOthersJoinAndRebalanceOccursThenMessagesShouldNotBeDuplicated_BackgorundThreadsReceivedMessages);
            HashSet<string> resultSet = new HashSet<string>();
            int nrOfDuplicates = 0;
            foreach (var receivedMessage in receivedMessages)
            {
                var msgString = Encoding.UTF8.GetString(receivedMessage.Payload);
                if (resultSet.Contains(msgString))
                {
                    nrOfDuplicates++;
                }
                else
                {
                    resultSet.Add(msgString);
                }
            }

            int totalMessagesFromAllThreads = receivedMessages.Count;
            Assert.AreEqual(originalNrOfMessages, totalMessagesFromAllThreads);

            Assert.AreEqual(0, nrOfDuplicates);
        }

        void WhenConsumerConsumesAndLaterOthersJoinAndRebalanceOccursThenMessagesShouldNotBeDuplicated_DoWork(object sender, DoWorkEventArgs e)
        {
            var consumerConfig = this.ZooKeeperBasedConsumerConfig;
            consumerConfig.FetchSize = 256;

            int resultMessages = 0;
            HashSet<string> resultSet = new HashSet<string>();
            int nrOfDuplicates = 0;
            StringBuilder sb = new StringBuilder();
            var receivedMessages = new List<Message>();
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
                            receivedMessages.Add(message);
                            var msgString = Encoding.UTF8.GetString(message.Payload);
                            sb.AppendLine(msgString);
                            if (resultSet.Contains(msgString))
                            {
                                nrOfDuplicates++;
                            }
                            else
                            {
                                resultSet.Add(msgString);
                            }
                            resultMessages++;
                        }
                    }
                }
                catch (ConsumerTimeoutException)
                {
                    // do nothing, this is expected
                }
            }
            var threadId = Thread.CurrentThread.ManagedThreadId.ToString();
            using (StreamWriter outfile = new StreamWriter("ConsumerTestDumpThread-"+threadId+".txt"))
            {
                outfile.Write(sb.ToString());
            }
            lock (WhenConsumerConsumesAndLaterOthersJoinAndRebalanceOccursThenMessagesShouldNotBeDuplicated_BackgorundThreadsResultCounterLock)
            {
                WhenConsumerConsumesAndLaterOthersJoinAndRebalanceOccursThenMessagesShouldNotBeDuplicated_BackgorundThreadsReceivedMessages.AddRange(receivedMessages);
                WhenConsumerConsumesAndLaterOthersJoinAndRebalanceOccursThenMessagesShouldNotBeDuplicated_BackgorundThreadsDoneNr++;
            }
            
        }
    }
}
