using System;

namespace Kafka.Client.Common
{
    /// <summary>
    /// Indicates the client has requested a range no longer available on the server
    /// </summary>
    public class UnknownCodecException : Exception
    {
        public UnknownCodecException()
            : base()
        {
        }

        public UnknownCodecException(string message)
            : base(message)
        {
        }
    }
}