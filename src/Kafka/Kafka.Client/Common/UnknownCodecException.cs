namespace Kafka.Client.Common
{
    using System;

    /// <summary>
    /// Indicates the client has requested a range no longer available on the server
    /// </summary>
    public class UnknownCodecException : Exception
    {
        public UnknownCodecException()
        {
        }

        public UnknownCodecException(string message)
            : base(message)
        {
        }
    }
}