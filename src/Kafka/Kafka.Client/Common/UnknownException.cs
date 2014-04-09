namespace Kafka.Client.Common
{
    using System;

    public class UnknownException : Exception
    {
        public UnknownException()
        {
        }

        public UnknownException(string message)
            : base(message)
        {
        }
    }
}