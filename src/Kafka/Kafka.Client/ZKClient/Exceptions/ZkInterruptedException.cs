namespace Kafka.Client.ZKClient.Exceptions
{
    using System;

    public class ZkInterruptedException : Exception
    {
        public ZkInterruptedException()
        {
        }

        public ZkInterruptedException(string message)
            : base(message)
        {
        }

        public ZkInterruptedException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public ZkInterruptedException(Exception innerException)
            : base(string.Empty, innerException)
        {
        }



    }
}