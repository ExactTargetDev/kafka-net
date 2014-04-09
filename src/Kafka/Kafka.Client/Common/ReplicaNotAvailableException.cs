namespace Kafka.Client.Common
{
    using System;

    public class ReplicaNotAvailableException : Exception
    {
        public ReplicaNotAvailableException()
        {
        }

        public ReplicaNotAvailableException(string message)
            : base(message)
        {
        }
    }
}