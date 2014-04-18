namespace Kafka.Client.ZKClient
{
    using System;

    public class ZkEvent
    {
        private readonly string description;

        public ZkEvent(string description)
        {
            this.description = description;
        }

        public Action RunAction { get; set; }

        public override string ToString()
        {
            return string.Format("Description: {0}", this.description);
        }
    }
}