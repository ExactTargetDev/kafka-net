namespace Kafka.Client.Common
{
    using System;

    /// <summary>
    /// Indicates a producer pool initialization problem
    /// </summary>
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