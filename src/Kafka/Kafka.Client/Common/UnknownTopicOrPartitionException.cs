namespace Kafka.Client.Common
{
    using System;

    public class UnknownTopicOrPartitionException : Exception
    {
        public UnknownTopicOrPartitionException()
        {
        }

        public UnknownTopicOrPartitionException(string message)
            : base(message)
        {
        }
    }
}