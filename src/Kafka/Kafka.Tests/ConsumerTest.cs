namespace Kafka.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    using Kafka.Client.Cfg;
    using Kafka.Client.Consumers;

    using Xunit;

    public class ConsumerTest
    {
        [Fact]
        public void Test()
        {
            var config = new ConsumerConfiguration("192.168.1.14", 2181);
            var consumer = Consumer.Create(config);
            var topic = new Dictionary<string, int>
                            {
                               { "topic5", 1 }
                            };
            var messageStreams = consumer.CreateMessageStreams(topic);
            var topic5Stream = messageStreams["topic5"];
            foreach (var stream in topic5Stream)
            {
                foreach (var message in stream)
                {
                    Console.WriteLine(Encoding.UTF8.GetString(message.Key));
                }
            }
        }
    }
}