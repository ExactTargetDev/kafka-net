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
            var config = new ConsumerConfiguration("192.168.1.14", 2181, "group5");
            config.ClientId = "client1244";
            config.ZooKeeper.ZkSessionTimeoutMs = 3 * 60 * 1000;
            config.FetchWaitMaxMs = 5000;
            var consumer = Consumer.Create(config);
            var topic = new Dictionary<string, int>
                            {
                                { "t1", 1 }
                            };
            var messageStreams = consumer.CreateMessageStreams(topic);
            var topic5Stream = messageStreams["t1"];

            Task.Factory.StartNew(
                () =>
                    {
                        Console.WriteLine("Starting fetcher thread");
                        foreach (var stream in topic5Stream)
                        {
                            foreach (var message in stream)
                            {
                                Console.WriteLine(Encoding.UTF8.GetString(message.Key));
                            }
                        }
                    });

            Thread.Sleep(TimeSpan.FromSeconds(15)); //TODO: delete me
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