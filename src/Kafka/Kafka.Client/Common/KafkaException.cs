namespace Kafka.Client.Common
{
    using System;

    /// <summary>
    ///  Generic Kafka exception
    /// </summary>
    public class KafkaException : Exception
    {
        public KafkaException()
        {
        }

        public KafkaException(string message)
            : base(message)
        {
        }

        public KafkaException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}