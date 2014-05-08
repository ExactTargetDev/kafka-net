namespace Kafka.Client.Common
{
    using System;

    /// <summary>
    /// If we don't know what else it is, call it this
    /// </summary>
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