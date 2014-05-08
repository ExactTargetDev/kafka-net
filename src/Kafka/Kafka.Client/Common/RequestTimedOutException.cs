namespace Kafka.Client.Common
{
    using System;

    /// <summary>
    /// Thrown when a produce request times out - i.e., if one or more partitions it
    /// sends messages to receives fewer than the requiredAcks that is specified in
    /// the produce request.
    /// </summary>
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