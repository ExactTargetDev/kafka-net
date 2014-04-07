namespace Kafka.Client.Common
{
    using System;

    public class MessageSizeTooLargeException : Exception
    {
        public MessageSizeTooLargeException()
        {
        }

        public MessageSizeTooLargeException(string message)
            : base(message)
        {
        }
    }
}