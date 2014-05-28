namespace Kafka.Client.Common
{
    /// <summary>
    /// Convenience case class since (clientId, topic) pairs are used in the creation
    /// of many Stats objects.
    /// </summary>
    public class ClientIdAndTopic
    {
        public readonly string ClientId;

        public readonly string Topic;

        public ClientIdAndTopic(string clientId, string topic)
        {
            this.ClientId = clientId;
            this.Topic = topic;
        }

        protected bool Equals(ClientIdAndTopic other)
        {
            return this.ClientId == other.ClientId && this.Topic == other.Topic;
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

            return this.Equals((ClientIdAndTopic)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((this.ClientId != null ? this.ClientId.GetHashCode() : 0) * 397) ^ (this.Topic != null ? this.Topic.GetHashCode() : 0);
            }
        }

        public override string ToString()
        {
            return string.Format("({0}-{1})", this.ClientId, this.Topic);
        }
    }
}