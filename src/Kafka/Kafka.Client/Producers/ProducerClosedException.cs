namespace Kafka.Client.Producers
{
    using System;

    public class ProducerClosedException : Exception
    {
        public ProducerClosedException() : base("Producer already closed")
        {
        }
    }
}