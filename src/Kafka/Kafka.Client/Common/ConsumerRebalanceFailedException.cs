namespace Kafka.Client.Common
{
    using System;

    /// <summary>
    /// Thrown when a request is made for broker but no brokers with that topic
    /// exist.
    /// </summary>
    public class ConsumerRebalanceFailedException : Exception
    {
        public ConsumerRebalanceFailedException()
        {
        }

        public ConsumerRebalanceFailedException(string message)
            : base(message)
        {
        }

        public ConsumerRebalanceFailedException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}