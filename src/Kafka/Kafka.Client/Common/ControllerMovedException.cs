namespace Kafka.Client.Common
{
    using System;

    public class ControllerMovedException : Exception
    {
        public ControllerMovedException()
        {
        }

        public ControllerMovedException(string message)
            : base(message)
        {
        }
    }
}