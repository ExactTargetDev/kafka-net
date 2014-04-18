namespace Kafka.Client.ZKClient.Exceptions
{
    using System;

    public class ZkBadVersionException : ZkException
    {
        public ZkBadVersionException()
        {
        }

        public ZkBadVersionException(string message)
            : base(message)
        {
        }

        public ZkBadVersionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}