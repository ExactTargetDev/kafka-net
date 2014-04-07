namespace Kafka.Tests
{
    using System;
    using System.Configuration;
    using System.Text;

    using Kafka.Client.Cfg;
    using Kafka.Client.Producers;

    using Xunit;

    public class ProducerTest
    {
        [Fact]
         public void Produce()
        {
            Console.WriteLine(typeof(DefaultPartitioner).FullName);

            var section = ConfigurationManager.GetSection("kafkaProducer") as ProducerConfigurationSection;
            var config = new ProducerConfiguration(section);
            var producer = new Producer<byte[], byte[]>(config);

            var msg = new KeyedMessage<byte[], byte[]>("topic1", Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));

            producer.Send(msg);

            //TODO finish me
        }
    }
}