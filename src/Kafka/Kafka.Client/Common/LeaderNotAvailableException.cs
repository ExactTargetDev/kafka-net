namespace Kafka.Client.Common
{
    using System;

    public class LeaderNotAvailableException : Exception
    {
        public LeaderNotAvailableException()
        {
        }

        public LeaderNotAvailableException(string message)
            : base(message)
        {
        }
    }
}