namespace Kafka.Client.Common
{
    using System;

    /// <summary>
    /// Thrown when a request is made for partition on a broker that is NOT a leader for that partition
    /// </summary>
    public class NotLeaderForPartitionException : Exception
    {
        public NotLeaderForPartitionException()
        {
        }

        public NotLeaderForPartitionException(string message)
            : base(message)
        {
        }
    }
}