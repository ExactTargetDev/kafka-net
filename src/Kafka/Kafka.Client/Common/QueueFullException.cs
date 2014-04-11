namespace Kafka.Client.Common
{
    using System;

    public class QueueFullException : Exception
    {
        public QueueFullException()
        {
        }

        public QueueFullException(string message)
            : base(message)
        {
        }
    }
}