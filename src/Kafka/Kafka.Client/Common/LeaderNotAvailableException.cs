namespace Kafka.Client.Common
{
    using System;

    /// <summary>
    /// Thrown when a request is made for partition, but no leader exists for that partition
    /// </summary>
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