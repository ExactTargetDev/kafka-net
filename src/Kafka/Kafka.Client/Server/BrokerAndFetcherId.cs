namespace Kafka.Client.Server
{
    using Kafka.Client.Clusters;

    public class BrokerAndFetcherId
    {
        public Broker Broker { get; private set; }

        public int FetcherId { get; private set; }

        public BrokerAndFetcherId(Broker broker, int fetcherId)
        {
            this.Broker = broker;
            this.FetcherId = fetcherId;
        }

        protected bool Equals(BrokerAndFetcherId other)
        {
            return Equals(this.Broker, other.Broker) && this.FetcherId == other.FetcherId;
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
            return Equals((BrokerAndFetcherId)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((this.Broker != null ? this.Broker.GetHashCode() : 0) * 397) ^ this.FetcherId;
            }
        }
    }
}