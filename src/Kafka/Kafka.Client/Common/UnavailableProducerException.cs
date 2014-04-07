namespace Kafka.Client.Common
{
    using System;

    /// <summary>
    /// Indicates a producer pool initialization problem
    /// </summary>
    public class UnavailableProducerException : Exception
    {
        public UnavailableProducerException()
        {
        }

        public UnavailableProducerException(string message)
            : base(message)
        {
        }
    }
}