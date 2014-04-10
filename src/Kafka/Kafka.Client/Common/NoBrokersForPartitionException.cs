namespace Kafka.Client.Common
{
    using System;

    public class NoBrokersForPartitionException : Exception
    {
        public NoBrokersForPartitionException()
        {
        }

        public NoBrokersForPartitionException(string message)
            : base(message)
        {
        }
    }
}