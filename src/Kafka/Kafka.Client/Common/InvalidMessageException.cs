using System;

namespace Kafka.Client.Common
{
    /// <summary>
    /// Indicates that a message failed its checksum and is corrupt
    /// </summary>
    public class InvalidMessageException : Exception
    {
        public InvalidMessageException()
            : base()
        {
        }

        public InvalidMessageException(string message)
            : base(message)
        {
        }
    }
}