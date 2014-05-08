namespace Kafka.Client.Common
{
    using System;

    /// <summary>
    /// Thrown when a request is made for partition, but no leader exists for that partition
    /// </summary>
    public class ReplicaNotAvailableException : Exception
    {
        public ReplicaNotAvailableException()
        {
        }

        public ReplicaNotAvailableException(string message)
            : base(message)
        {
        }
    }
}