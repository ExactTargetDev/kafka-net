namespace Kafka.Tests.Consumers
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;

    using Kafka.Client.Cfg;
    using Kafka.Client.Producers;
    using Kafka.Tests.Custom.Server;
    using Kafka.Tests.Integration;
    using Kafka.Tests.Utils;

    using Xunit;

    public class ConsumerIteratorTest : KafkaServerTestHarness
    {
        private const int NumNodes = 1;

        protected override List<TempKafkaConfig> CreateConfigs()
        {
            return TestUtils.CreateBrokerConfigs(
                NumNodes,
                idx => new Dictionary<string, string>
                           {
                               {"zookeeper.connect", "localhost:" + TestZkUtils.ZookeeperPort }
                           });
        }

        [Fact]
        public void TestConsumerIteratorDeduplicationDeepIterator()
        {
            Thread.Sleep(10000);
        }

        //TODO: fisnih me
    }
}