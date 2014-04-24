namespace Kafka.Client.Common
{
    using System;

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