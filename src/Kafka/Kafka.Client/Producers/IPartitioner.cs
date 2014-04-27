namespace Kafka.Client.Producers
{
    /// <summary>
    /// A partitioner controls the mapping between user-provided keys and kafka partitions. Users can implement a custom
    /// partitioner to change this mapping.
    /// 
    /// Implementations will be constructed via reflection and are required to have a constructor that takes a single
    /// VerifiableProperties instance--this allows passing configuration properties into the partitioner implementation.
    /// </summary>
    public interface IPartitioner
    {
        /// <summary>
        /// Uses the key to calculate a partition bucket id for routing
        /// the Data to the appropriate broker partition
        /// </summary>
        /// <param name="key"></param>
        /// <param name="numPartitions"></param>
        /// <returns>an integer between 0 and numPartitions-1</returns>
        int Partition(object key, int numPartitions);
    }
}