namespace Kafka.Client.Common
{
    using System;

    /// <summary>
    /// Indicates the client has specified offset metadata that exceeds the configured
    /// maximum size in bytes
    /// </summary>
    public class OffsetMetadataTooLargeException : Exception
    {
        public OffsetMetadataTooLargeException()
        {
        }

        public OffsetMetadataTooLargeException(string message)
            : base(message)
        {
        }
    }
}