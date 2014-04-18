namespace Kafka.Client.ZKClient.Exceptions
{
    using System;

    public class ZkNoNodeException : ZkException
    {
        public ZkNoNodeException()
        {
        }

        public ZkNoNodeException(string message)
            : base(message)
        {
        }

        public ZkNoNodeException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}