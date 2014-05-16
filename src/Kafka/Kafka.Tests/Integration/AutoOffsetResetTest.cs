namespace Kafka.Tests.Integration
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Text;

    using Kafka.Client.Consumers;
    using Kafka.Client.Producers;
    using Kafka.Client.Serializers;
    using Kafka.Client.Utils;
    using Kafka.Tests.Custom.Server;
    using Kafka.Tests.Utils;

    using log4net;

    using Xunit;

    public class AutoOffsetResetTest : KafkaServerTestHarness
    {
        protected static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public const string Topic = "test_topic";

        public const string Group = "default_group";

        public const string TestConsumer = "consumer";

        public const int NumMessages = 10;

        public const int LargeOffset = 10000;

        public const int SmallOffset = -1;

        protected override List<TempKafkaConfig> CreateConfigs()
        {
            return TestUtils.CreateBrokerConfigs(1);
        }

        [Fact]
        public void TestResetToEarliestWhenOffsetTooHigh()
        {
            Assert.Equal(NumMessages, ResetAndConsume(NumMessages, "smallest", LargeOffset));
        }

        [Fact]
        public void TestResetToEarliestWhenOffsetTooLow()
        {
            Assert.Equal(NumMessages, ResetAndConsume(NumMessages , "smallest", SmallOffset));
        }

        [Fact]
        public void TestResetToLatestWhenOffsetTooHigh()
        {
            Assert.Equal(0, ResetAndConsume(NumMessages, "largest", LargeOffset));
        }

        [Fact]
        public void TestResetToLatestWhenOffsetTooLow()
        {
            Assert.Equal(0, ResetAndConsume(NumMessages, "largest", SmallOffset));
        }

        /// <summary>
        /// Produce the given number of messages, create a consumer with the given offset policy, 
        /// then reset the offset to the given value and consume until we get no new messages. 
        /// </summary>
        /// <param name="numMessages"></param>
        /// <param name="resetTo"></param>
        /// <param name="offset"></param>
        /// <returns>The count of messages received.</returns>
        public int ResetAndConsume(int numMessages, string resetTo, long offset)
        {
            TestUtils.WaitUntilLeaderIsElectedOrChanged(ZkClient, Topic, 0, 1000);


            var producer = TestUtils.CreateProducer(
                TestUtils.GetBrokerListFromConfigs(Configs), new DefaultEncoder(), new StringEncoder());

            for (var i = 0; i < numMessages; i++)
            {
                producer.Send(new KeyedMessage<string, byte[]>(Topic, Topic, Encoding.UTF8.GetBytes("test")));
            }

            TestUtils.WaitUntilMetadataIsPropagated(Servers, Topic, 0, 1000);

            // update offset in zookeeper for consumer to jump "forward" in time
            var dirs = new ZKGroupTopicDirs(Group, Topic);
            var consumerConfig = TestUtils.CreateConsumerProperties(ZkConnect, Group, TestConsumer);
            consumerConfig.AutoOffsetReset = resetTo;
            consumerConfig.ConsumerTimeoutMs = 2000;
            consumerConfig.FetchWaitMaxMs = 0;

            TestUtils.UpdateConsumerOffset(consumerConfig, dirs.ConsumerOffsetDir + "/" + "0", offset);
            Logger.InfoFormat("Update consumer offset to {0}", offset);

            var consumerConnector = Consumer.Create(consumerConfig);
            var messagesStream = consumerConnector.CreateMessageStreams(new Dictionary<string, int> { { Topic, 1 } })[Topic].First();

            var received = 0;
            var iter = messagesStream.GetEnumerator();
            try
            {
                for (var i = 0; i < numMessages; i++)
                {
                    iter.MoveNext(); // will throw a timeout exception if the message isn't there
                    received ++;
                }
            }
            catch (ConsumerTimeoutException)
            {
                Logger.InfoFormat("consumer timeout out after receiving {0} messages", received);
            }
            finally
            {
                producer.Dispose();
                consumerConnector.Shutdown();
            }
            return received;
        }
    }
}