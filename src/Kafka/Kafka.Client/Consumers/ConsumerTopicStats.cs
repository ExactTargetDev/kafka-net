namespace Kafka.Client.Consumers
{
    using System;

    public class ConsumerTopicStats
    {
        public string ClientId { get; set; }

        public ConsumerTopicStats(string clientId)
        {
            this.ClientId = clientId;
        }

        public ConsumerTopicStats GetConsumerAllTopicStats()
        {
            throw new NotImplementedException();
        }

        public ConsumerTopicStats GetConsumerTopicStats(string topic)
        {
            throw new NotImplementedException();
        }


        //TODO: finish me
    }
}