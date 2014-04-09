namespace Kafka.Client.Common
{
    using System;

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