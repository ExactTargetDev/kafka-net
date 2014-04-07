namespace Kafka.Client.Common
{
    using System;

    public class BrokerNotAvailableException : Exception
    {
        public BrokerNotAvailableException()
        {
        }

        public BrokerNotAvailableException(string message)
            : base(message)
        {
        }
    }
}