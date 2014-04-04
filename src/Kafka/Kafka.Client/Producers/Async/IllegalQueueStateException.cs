namespace Kafka.Client.Producers.Async
{
    using System;

    public class IllegalQueueStateException : Exception
    {
        public IllegalQueueStateException()
            : base()
        {
        }

        public IllegalQueueStateException(string message)
            : base(message)
        {
        }
         
    }
}