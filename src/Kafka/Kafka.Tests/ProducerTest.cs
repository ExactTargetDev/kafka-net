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

            var section = ConfigurationManager.GetSection("kafkaProducer3") as ProducerConfigurationSection;
            var config = new ProducerConfig(section);
            using (var producer = new Producer<byte[], byte[]>(config))
            {
                for (int i = 0; i < 50; i++)
                {
                    var msg = new KeyedMessage<byte[], byte[]>(
                        "topic2", Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value" + i));


                    producer.Send(msg);
                }
            }

        }
    }
}