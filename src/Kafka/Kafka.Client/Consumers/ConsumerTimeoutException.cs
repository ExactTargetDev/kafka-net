namespace Kafka.Client.Consumers
{
    using System;

    public class ConsumerTimeoutException : Exception
    {
        public ConsumerTimeoutException()
        {
        }

        public ConsumerTimeoutException(string message)
            : base(message)
        {
        }
    }
}