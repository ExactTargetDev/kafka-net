namespace Kafka.Tests.Integration
{
    using Kafka.Client.Consumers;
    using Kafka.Client.Producers;
    using Kafka.Tests.Utils;

    using System.Linq;

    public abstract class ProducerConsumerTestHarness : KafkaServerTestHarness
    {
        private readonly int port;

        private readonly string host = "localhost";

        protected Producer<string, string> Producer { get; private set; }

        protected SimpleConsumer Consumer { get; private set; }

        protected ProducerConsumerTestHarness()
        {
            this.port = Configs.First().Port;
            var props = TestUtils.GetProducerConfig(
                TestUtils.GetBrokerListFromConfigs(Configs), typeof(StaticPartitioner).AssemblyQualifiedName);
            this.Producer = new Producer<string, string>(props);
            this.Consumer = new SimpleConsumer(host, port, 1000000, 64 * 1024, string.Empty);
        }

        public override void Dispose()
        {
            base.Dispose();
            Producer.Dispose();
            Consumer.Close();
        }
    }
}