namespace Kafka.Client.Server
{
    using Kafka.Client.Clusters;

    public class BrokerAndInitialOffset
    {
        public Broker Broker { get; private set; }

        public long InitOffset { get; private set; }

        public BrokerAndInitialOffset(Broker broker, long initOffset)
        {
            this.Broker = broker;
            this.InitOffset = initOffset;
        }

        protected bool Equals(BrokerAndInitialOffset other)
        {
            return Equals(this.Broker, other.Broker) && this.InitOffset == other.InitOffset;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }
            if (ReferenceEquals(this, obj))
            {
                return true;
            }
            if (obj.GetType() != this.GetType())
            {
                return false;
            }
            return Equals((BrokerAndInitialOffset)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((this.Broker != null ? this.Broker.GetHashCode() : 0) * 397) ^ this.InitOffset.GetHashCode();
            }
        }
    }
}