namespace Kafka.Client.Common
{
    using System;

    public class FailedToSendMessageException : Exception
    {
        public FailedToSendMessageException()
        {
        }

        public FailedToSendMessageException(string message)
            : base(message)
        {
        }
    }
}