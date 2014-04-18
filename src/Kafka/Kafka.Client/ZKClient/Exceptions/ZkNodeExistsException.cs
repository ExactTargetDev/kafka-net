namespace Kafka.Client.ZKClient.Exceptions
{
    using System;

    public class ZkNodeExistsException : ZkException
    {
        public ZkNodeExistsException()
        {
        }

        public ZkNodeExistsException(string message)
            : base(message)
        {
        }

        public ZkNodeExistsException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}