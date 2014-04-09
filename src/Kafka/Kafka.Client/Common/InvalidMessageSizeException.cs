namespace Kafka.Client.Common
{
    using System;

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