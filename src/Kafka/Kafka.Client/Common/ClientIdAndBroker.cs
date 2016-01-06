namespace Kafka.Client.Common
{
    /// <summary>
    /// Convenience case class since (clientId, brokerInfo) pairs are used to create
    /// SyncProducer Request Stats and SimpleConsumer Request and Response Stats.
    /// </summary>
    public class ClientIdAndBroker
    {
        public string ClientId { get; private set; }

        public string BrokerInfo { get; private set; }

        public ClientIdAndBroker(string clientId, string brokerInfo)
        {
            this.ClientId = clientId;
            this.BrokerInfo = brokerInfo;
        }

        public override string ToString()
        {
            return string.Format("{0}-{1}", this.ClientId, this.BrokerInfo);
        }

        protected bool Equals(ClientIdAndBroker other)
        {
            return this.ClientId == other.ClientId && this.BrokerInfo == other.BrokerInfo;
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

            return this.Equals((ClientIdAndBroker)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((this.ClientId != null ? this.ClientId.GetHashCode() : 0) * 397) ^ (this.BrokerInfo != null ? this.BrokerInfo.GetHashCode() : 0);
            }
        }
    }
}