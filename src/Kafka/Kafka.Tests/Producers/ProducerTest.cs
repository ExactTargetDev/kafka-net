namespace Kafka.Tests.Producers
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using System.Threading;

    using Kafka.Client.Admin;
    using Kafka.Client.Api;
    using Kafka.Client.Cfg;
    using Kafka.Client.Common;
    using Kafka.Client.Consumers;
    using Kafka.Client.Extensions;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Serializers;
    using Kafka.Client.Utils;
    using Kafka.Tests.Custom.Server;
    using Kafka.Tests.Utils;
    using Kafka.Tests.Zk;

    using Xunit;

    public class ProducerTest : ZooKeeperTestHarness
    {
        private const int BrokerId1 = 0;

        private const int BrokerId2 = 1;

        private readonly List<int> ports = TestUtils.ChoosePorts(2);

        private readonly int port1;

        private readonly int port2;

        private Process server1;

        private readonly Process server2;

        private readonly SimpleConsumer consumer1;

        private readonly SimpleConsumer consumer2;

        private readonly List<Process> servers = new List<Process>();

        private readonly TempKafkaConfig config1;

        private readonly TempKafkaConfig config2;

        public ProducerTest()
        {
            this.ports = TestUtils.ChoosePorts(2);
            this.port1 = this.ports[0];
            this.port2 = this.ports[1];
            this.config1 = TestUtils.CreateBrokerConfig(
                BrokerId1, this.port1, idx => new Dictionary<string, string> { { "num.partitons", "4" } });

            this.config2 = TestUtils.CreateBrokerConfig(
                BrokerId2, this.port2, idx => new Dictionary<string, string> { { "num.partitons", "4" } });

            this.server1 = this.StartServer(this.config1);
            this.server2 = this.StartServer(this.config2);

            this.servers = new List<Process> { this.server1, this.server2 };

            this.consumer1 = new SimpleConsumer("localhost", this.port1, 1000000, 64 * 1024, string.Empty);
            this.consumer2 = new SimpleConsumer("localhost", this.port2, 100, 64 * 1024, string.Empty);

            this.WaitForServersToSettle();
        }

        public override void Dispose()
        {
            base.Dispose();
            Util.Swallow(() =>
                {
                    using (this.server1)
                    {
                        this.server1.Kill();
                    }
                });

            Util.Swallow(() =>
            {
                using (this.server2)
                {
                    this.server2.Kill();
                }
            });

            Util.Swallow(() => this.config1.Dispose());
            Util.Swallow(() => this.config2.Dispose());
        }

        private Process StartServer(TempKafkaConfig config)
        {
            return KafkaRunClassHelper.Run(KafkaRunClassHelper.KafkaServerMainClass, config.ConfigLocation);
        }

        public void WaitForServersToSettle()
        {
            foreach (TempKafkaConfig config in new List<TempKafkaConfig> { this.config1, this.config2 })
            {
                if (!ZkClient.WaitUntilExists(ZkUtils.BrokerIdsPath + "/" + config.BrokerId, TimeSpan.FromSeconds(5)))
                {
                    throw new Exception("Timeout on waiting for broker " + config.BrokerId + " to settle");
                }
            }
        }

        [Fact]
        public void TestUpdateBrokerPartitionInfo()
        {
            var topic = "new-topic";
            AdminUtils.CreateTopic(this.ZkClient, topic, 1, 2, new Dictionary<string, string>());

            // wait until the update metadata request for new topic reaches all servers
            TestUtils.WaitUntilMetadataIsPropagated(this.servers, topic, 0, 500);
            TestUtils.WaitUntilLeaderIsElectedOrChanged(this.ZkClient, topic, 0, 500);

            var producerConfig1 = new ProducerConfig();
            producerConfig1.Brokers = new List<BrokerConfiguration>
                                          {
                                              new BrokerConfiguration
                                                  {
                                                     BrokerId = 0,
                                                     Host = "localhost",
                                                     Port = 80
                                                  },
                                                  new BrokerConfiguration
                                                      {
                                                          BrokerId = 1,
                                                          Host = "localhost",
                                                          Port = 81
                                                      }
                                          };
            producerConfig1.KeySerializer = typeof(StringEncoder).AssemblyQualifiedName;
            producerConfig1.Serializer = typeof(StringEncoder).AssemblyQualifiedName;
            var producer1 = new Producer<string, string>(producerConfig1);
            try
            {
                producer1.Send(new KeyedMessage<string, string>(topic, "test", "test1"));
                Assert.False(true, "Test should fail because the broker list provided are not valid");
            }
            catch (FailedToSendMessageException)
            {
                // ok
            }
            finally
            {
                producer1.Dispose();
            }
            
            var producerConfig2 = new ProducerConfig();
            producerConfig2.Brokers = new List<BrokerConfiguration>
                                          {
                                             new BrokerConfiguration
                                                  {
                                                     BrokerId = 0,
                                                     Host = "localhost",
                                                     Port = 80
                                                  },
                                                  new BrokerConfiguration
                                                      {
                                                          BrokerId = 1,
                                                          Host = "localhost",
                                                          Port = this.port1
                                                      }
                                          };
            producerConfig2.KeySerializer = typeof(StringEncoder).AssemblyQualifiedName;
            producerConfig2.Serializer = typeof(StringEncoder).AssemblyQualifiedName;
            var producer2 = new Producer<string, string>(producerConfig2);
            try
            {
                producer2.Send(new KeyedMessage<string, string>(topic, "test", "test1"));
            }
            finally
            {
                producer2.Dispose();
            }

            var producerConfig3 = new ProducerConfig();
            producerConfig3.Brokers = new List<BrokerConfiguration>
                                          {
                                             new BrokerConfiguration
                                                  {
                                                     BrokerId = 0,
                                                     Host = "localhost",
                                                     Port = this.port1
                                                  },
                                                  new BrokerConfiguration
                                                      {
                                                          BrokerId = 1,
                                                          Host = "localhost",
                                                          Port = this.port2
                                                      }
                                          };
            producerConfig3.KeySerializer = typeof(StringEncoder).AssemblyQualifiedName;
            producerConfig3.Serializer = typeof(StringEncoder).AssemblyQualifiedName;
            var producer3 = new Producer<string, string>(producerConfig2);
            try
            {
                producer3.Send(new KeyedMessage<string, string>(topic, "test", "test1"));
            }
            finally
            {
                producer3.Dispose();
            }
        }

        [Fact]
        public void TestSendToNewTopic()
        {
            var producerConfig1 = new ProducerConfig
                                      {
                                          Serializer = typeof(StringEncoder).AssemblyQualifiedName,
                                          KeySerializer = typeof(StringEncoder).AssemblyQualifiedName,
                                          PartitionerClass =
                                              typeof(StaticPartitioner).AssemblyQualifiedName,
                                          Brokers =
                                              TestUtils.GetBrokerListFromConfigs(
                                                  new List<TempKafkaConfig> { this.config1, this.config2 }),
                                          RequestRequiredAcks = 2,
                                          RequestTimeoutMs = 1000
                                      };

            var producerConfig2 = new ProducerConfig
                                      {
                                          Serializer = typeof(StringEncoder).AssemblyQualifiedName,
                                          KeySerializer = typeof(StringEncoder).AssemblyQualifiedName,
                                          PartitionerClass =
                                              typeof(StaticPartitioner).AssemblyQualifiedName,
                                          Brokers =
                                              TestUtils.GetBrokerListFromConfigs(
                                                  new List<TempKafkaConfig> { this.config1, this.config2 }),
                                          RequestRequiredAcks = 3,
                                          RequestTimeoutMs = 1000
                                      };

            var topic = "new-topic";

            // create topic with 1 partition and await leadership
            AdminUtils.CreateTopic(this.ZkClient, topic, 1, 2, new Dictionary<string, string>());
            TestUtils.WaitUntilMetadataIsPropagated(this.servers, topic, 0, 1000);
            TestUtils.WaitUntilLeaderIsElectedOrChanged(this.ZkClient, topic, 0, 500);

            var producer1 = new Producer<string, string>(producerConfig1);
            var producer2 = new Producer<string, string>(producerConfig2);

            // Available partition ids should be 0.
            producer1.Send(new KeyedMessage<string, string>(topic, "test", "test1"));
            producer1.Send(new KeyedMessage<string, string>(topic, "test", "test2"));

            // get the leader
            var leaderOpt = ZkUtils.GetLeaderForPartition(ZkClient, topic, 0);
            Assert.True(leaderOpt.HasValue);

            var leader = leaderOpt.Value;

            var messageSet = (leader == this.config1.BrokerId)
                                 ? this.consumer1.Fetch(new FetchRequestBuilder().AddFetch(topic, 0, 0, 10000).Build())
                                            .MessageSet("new-topic", 0)
                                            .Iterator()
                                            .ToEnumerable()
                                            .ToList()
                                 : this.consumer2.Fetch(new FetchRequestBuilder().AddFetch(topic, 0, 0, 10000).Build())
                                            .MessageSet("new-topic", 0)
                                            .Iterator()
                                            .ToEnumerable()
                                            .ToList();
            Assert.Equal(2, messageSet.Count());
            Assert.Equal(new Message(Encoding.UTF8.GetBytes("test1"), Encoding.UTF8.GetBytes("test")), messageSet[0].Message);
            Assert.Equal(new Message(Encoding.UTF8.GetBytes("test2"), Encoding.UTF8.GetBytes("test")), messageSet[1].Message);
            producer1.Dispose();

            try
            {
                producer2.Send(new KeyedMessage<string, string>(topic, "test", "test2"));
                Assert.False(true, "Should have timed out for 3 acks.");
            }
            catch (FailedToSendMessageException)
            {
            }
            finally
            {
                producer2.Dispose();
            }
        }

        [Fact]
        public void TestSendWithDeadBroker()
        {
            var config = new ProducerConfig
            {
                Serializer = typeof(StringEncoder).AssemblyQualifiedName,
                KeySerializer = typeof(StringEncoder).AssemblyQualifiedName,
                PartitionerClass =
                    typeof(StaticPartitioner).AssemblyQualifiedName,
                Brokers =
                    TestUtils.GetBrokerListFromConfigs(
                        new List<TempKafkaConfig> { this.config1, this.config2 }),
                RequestRequiredAcks = 1,
                RequestTimeoutMs = 2000
            };

            var topic = "new-topic";

            // create topic
            AdminUtils.CreateOrUpdateTopicPartitionAssignmentPathInZK(
                this.ZkClient,
                topic,
                new Dictionary<int, List<int>>
                    {
                        { 0, new List<int> { 0 } },
                        { 1, new List<int> { 0 } },
                        { 2, new List<int> { 0 } },
                        { 3, new List<int> { 0 } },
                    }, 
                    new Dictionary<string, string>());

            // waiting for 1 partition is enought
            TestUtils.WaitUntilMetadataIsPropagated(this.servers, topic, 0, 1000);
            TestUtils.WaitUntilLeaderIsElectedOrChanged(this.ZkClient, topic, 0, 500);
            TestUtils.WaitUntilLeaderIsElectedOrChanged(this.ZkClient, topic, 1, 500);
            TestUtils.WaitUntilLeaderIsElectedOrChanged(this.ZkClient, topic, 2, 500);
            TestUtils.WaitUntilLeaderIsElectedOrChanged(this.ZkClient, topic, 3, 500);

            var producer = new Producer<string, string>(config);
            try
            {
                // Available partition ids should be 0, 1, 2 and 3, all lead and hosted only
                // on broker 0
                producer.Send(new KeyedMessage<string, string>(topic, "test", "test1"));
            }
            finally
            {
                Thread.Sleep(1000); // wait for server to fetch message from consumer
                // kill the server
                using (this.server1)
                {
                    this.server1.Kill();
                    SpinWait.SpinUntil(() => this.server1.HasExited, 500);
                }
            }

            try
            {
                // These sends should fail since there are no available brokers
                producer.Send(new KeyedMessage<string, string>(topic, "test", "test1"));
                Assert.True(false, "Should fail since no leader exists for the partition.");
            }
            catch
            {
            }

            // NOTE we can rewrite rest of test as we can't do the clean shutdown
            producer.Dispose();
        }

        [Fact]
        public void TestSendNullMessage()
        {
            var config = new ProducerConfig
                             {
                                 Serializer = typeof(StringEncoder).AssemblyQualifiedName,
                                 KeySerializer = typeof(StringEncoder).AssemblyQualifiedName,
                                 PartitionerClass = typeof(StaticPartitioner).AssemblyQualifiedName,
                                 Brokers =
                                     TestUtils.GetBrokerListFromConfigs(
                                         new List<TempKafkaConfig> { this.config1, this.config2 }),
                             };
            var producer = new Producer<string, string>(config);
           try
           {
               // create topic
               AdminUtils.CreateTopic(this.ZkClient, "new-topic", 2, 1, new Dictionary<string, string>());
               TestUtils.WaitUntilLeaderIsElectedOrChanged(this.ZkClient, "new-topic", 0, 500);

               producer.Send(new KeyedMessage<string, string>("new-topic", "key", null));
           }
           finally
           {
               producer.Dispose();
           }
        }
    }
}