namespace Kafka.Tests.Integration
{
    using System.Linq;

    using Kafka.Client.Consumers;
    using Kafka.Client.Producers;
    using Kafka.Tests.Utils;

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
            this.Consumer = new SimpleConsumer(this.host, this.port, 1000000, 64 * 1024, string.Empty);
        }

        public override void Dispose()
        {
            this.Producer.Dispose();
            this.Consumer.Close();
            base.Dispose();
        }
    }
}