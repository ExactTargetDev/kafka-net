namespace Kafka.Client.Common
{
    using System;

    /// <summary>
    /// Indicates the client has requested a range no longer available on the server
    /// </summary>
    public class OffsetOutOfRangeException : Exception
    {
        public OffsetOutOfRangeException()
        {
        }

        public OffsetOutOfRangeException(string message)
            : base(message)
        {
        }
    }
}