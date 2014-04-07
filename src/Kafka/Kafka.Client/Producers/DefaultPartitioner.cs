namespace Kafka.Client.Producers
{
    using System;

    using Kafka.Client.Cfg;

    public class DefaultPartitioner : IPartitioner
    {
        public DefaultPartitioner(ProducerConfig config)
        {
            
        }

        public int Partition(object key, int numPartitions)
        {
            return Math.Abs(key.GetHashCode()) % numPartitions;
        }
    }
}