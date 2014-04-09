namespace Kafka.Client.Common
{
    using System;

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