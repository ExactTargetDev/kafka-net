namespace Kafka.Client.Messages
{
    using System;

    /// <summary>
    /// Indicates that a message failed its checksum and is corrupt
    /// </summary>
    public class InvalidMessageException : Exception
    {
        public InvalidMessageException()
        {
        }

        public InvalidMessageException(string message)
            : base(message)
        {
        }
    }
}