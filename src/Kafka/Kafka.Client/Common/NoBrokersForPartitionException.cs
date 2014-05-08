namespace Kafka.Client.Common
{
    using System;

    /// <summary>
    /// Thrown when a request is made for broker but no brokers with that topic
    /// exist.
    /// </summary>
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