namespace Kafka.Client.Common
{
    using System;

    /// <summary>
    /// Indicates the queue for sending messages is full of unsent messages
    /// </summary>
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