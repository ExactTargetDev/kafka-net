namespace Kafka.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    using Kafka.Client.Api;
    using Kafka.Client.Cfg;
    using Kafka.Client.Consumers;

    using Xunit;

    public class ConsumerTest
    {
        public ConsumerTest()
        {
            //TODO: move to separate class and config statically
            log4net.Config.BasicConfigurator.Configure(
            new log4net.Appender.ConsoleAppender { Layout = new log4net.Layout.SimpleLayout() }
          );
        }

        [Fact]
        public void Test()
        {
            var config = new ConsumerConfiguration("192.168.1.14", 2181, "group1");
            var consumer = Consumer.Create(config);
            var topic = new Dictionary<string, int>
                            {
                               { "topic5", 1 }
                            };
            var messageStreams = consumer.CreateMessageStreams(topic);
            var topic5Stream = messageStreams["topic5"];

            Task.Factory.StartNew(
                () =>
                    {
                        foreach (var stream in topic5Stream)
                        {
                            foreach (var message in stream)
                            {
                                Console.WriteLine(Encoding.UTF8.GetString(message.Key));
                            }
                        }
                    });

            Thread.Sleep(10000);
            consumer.Shutdown();

        }

        [Fact]
        public void TestSimpleConsmer()
        {
            var consumer = new SimpleConsumer("192.168.1.14", 2181, 5000, 4096, "client-1");
            var request = new TopicMetadataRequest(new List<string> {"topic5"}, 5);
            var response = consumer.Send(request);

            consumer.Dispose();
        }
    }
}