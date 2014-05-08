namespace Kafka.Client.Common
{
    using System;

    /// <summary>
    /// Indicates an unknown topic or a partition id not between 0 and numPartitions-1
    /// </summary>
    public class UnknownTopicOrPartitionException : Exception
    {
        public UnknownTopicOrPartitionException()
        {
        }

        public UnknownTopicOrPartitionException(string message)
            : base(message)
        {
        }
    }
}