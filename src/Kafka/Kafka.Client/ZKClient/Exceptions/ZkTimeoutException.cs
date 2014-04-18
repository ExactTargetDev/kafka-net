namespace Kafka.Client.ZKClient.Exceptions
{
    using System;

    public class ZkTimeoutException : Exception
    {
        public ZkTimeoutException()
        {
        }

        public ZkTimeoutException(string message)
            : base(message)
        {
        }

        public ZkTimeoutException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}