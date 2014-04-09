namespace Kafka.Client.Common
{
    using System;

    public class RequestTimedOutException : Exception
    {
        public RequestTimedOutException()
        {
        }

        public RequestTimedOutException(string message)
            : base(message)
        {
        }
    }
}