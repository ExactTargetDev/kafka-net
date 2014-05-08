namespace Kafka.Client.Common
{
    using System;

    /// <summary>
    /// Indicates the client has requested a range no longer available on the server
    /// </summary>
    public class InvalidMessageSizeException : Exception
    {
        public InvalidMessageSizeException()
        {
        }

        public InvalidMessageSizeException(string message)
            : base(message)
        {
        }
    }
}