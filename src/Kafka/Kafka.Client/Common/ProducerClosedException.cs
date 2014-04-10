namespace Kafka.Client.Common
{
    using System;

    public class ProducerClosedException : Exception
    {
        public ProducerClosedException() : base("Producer already closed")
        {
        }
    }
}