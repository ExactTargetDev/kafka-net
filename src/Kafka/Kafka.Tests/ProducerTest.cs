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
        public ProducerTest()
        {
            log4net.Config.BasicConfigurator.Configure(
            new log4net.Appender.ConsoleAppender { Layout = new log4net.Layout.SimpleLayout() }
          );
        }

        [Fact]
        public void Produce()
        {
            var config = ProducerConfig.Configure("kafkaProducerSync");
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

        [Fact]
        public void ProduceStringMessage()
        {
            var config = ProducerConfig.Configure("kafkaProducerSyncString");
            using (var producer = new Producer<string, string>(config))
            {
                var stringIntMessage = new KeyedMessage<string, string>("topic7", "test", "testing");
                producer.Send(stringIntMessage);
            }

        }
    }
}