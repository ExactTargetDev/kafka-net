namespace Kafka.Client.Consumers
{
    public class ConsumerTopicStats
    {
        public string ClientId { get; set; }

        public ConsumerTopicStats(string clientId)
        {
            ClientId = clientId;
        }

        //TODO: 
    }
}