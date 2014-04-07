namespace Kafka.Client.Producers
{
    public class PartitionAndLeader
    {
        public string Topic { get; private set; }

        public int PartitionId { get; private set; }

        public int? LeaderBrokerIdOpt { get; private set; }

        public PartitionAndLeader(string topic, int partitionId, int? leaderBrokerIdOpt)
        {
            this.Topic = topic;
            this.PartitionId = partitionId;
            this.LeaderBrokerIdOpt = leaderBrokerIdOpt;
        }
    }
}