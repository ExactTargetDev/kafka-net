namespace Kafka.Client.Clusters
{
    using System;

    /// <summary>
    /// Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
    /// </summary>
    /// 
    /* TODO: Do we need it? 
    public class Partition
    {
        public string Topic { get; private set; }

        public int PartitionId { get; private set; }

        public int ReplicationFactor { get; private set; }

        public DateTime Time { get; set; }

        public ReplicaManager ReplicaManager { get; private set; }

        public Partition(string topic, int partitionId, int replicationFactor, DateTime time, ReplicaManager replicaManager)
        {
            this.Topic = topic;
            this.PartitionId = partitionId;
            this.ReplicationFactor = replicationFactor;
            this.Time = time;
            this.ReplicaManager = replicaManager;


            this.localBrokerId = replicaManager.config.BrokerId;
        }

        private readonly int localBrokerId;

        //TODO: finish me

    }*/
}