namespace Kafka.Client.Common
{
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
    }
}