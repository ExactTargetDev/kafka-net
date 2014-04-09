namespace Kafka.Client.Common
{
    using System;

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