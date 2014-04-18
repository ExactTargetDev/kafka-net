namespace Kafka.Client.ZKClient.Exceptions
{
    using System;

    public class ZkMarshallingError : Exception
    {
        public ZkMarshallingError()
        {
        }

        public ZkMarshallingError(string message)
            : base(message)
        {
        }

        public ZkMarshallingError(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}