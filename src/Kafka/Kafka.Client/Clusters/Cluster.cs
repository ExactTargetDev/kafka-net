namespace Kafka.Client.Clusters
{
    using System.Collections.Generic;

    using Kafka.Client.Extensions;

    /// <summary>
    ///  The set of active brokers in the cluster
    /// </summary>
    public class Cluster
    {
        private Dictionary<int, Broker> brokers = new Dictionary<int, Broker>();

        public Cluster(IEnumerable<Broker> brokers)
        {
            foreach (var broker in brokers)
            {
                this.brokers[broker.Id] = broker;
            }
        }

        public Cluster()
        {
            
        }


        public Broker GetBroker(int id)
        {
            Broker result;
            if (brokers.TryGetValue(id, out result))
            {
                return result;
            }
            return null;
        }

        public void Add(Broker broker)
        {
            this.brokers[broker.Id] = broker;
        }

        public void Remote(int id)
        {
            this.brokers.Remove(id);
        }

        public int Count
        {
            get
            {
                return this.brokers.Count;
            }
        }

        public override string ToString()
        {
            return string.Format("Cluster ({0})",  string.Join(", ", this.brokers.Values));
        }
    }
}