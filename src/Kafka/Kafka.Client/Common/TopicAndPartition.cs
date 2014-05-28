namespace Kafka.Client.Common
{
    using System;

    /// <summary>
    /// Convenience case class since (topic, partition) pairs are ubiquitous.
    /// </summary>
    public class TopicAndPartition
    {
        public readonly string Topic;

        public readonly int Partiton;

        private readonly int _hashCode;

        public TopicAndPartition(string topic, int partiton)
        {
            this.Topic = topic;
            this.Partiton = partiton;
            unchecked
            {
                this._hashCode = ((this.Topic != null ? this.Topic.GetHashCode() : 0) * 397) ^ this.Partiton;
            }
        }

        public TopicAndPartition(Tuple<string, int> tuple)
            : this(tuple.Item1, tuple.Item2)
        {
        }

        public Tuple<string, int> AsTuple()
        {
            return Tuple.Create(this.Topic, this.Partiton);
        }

        public override string ToString()
        {
            return string.Format("[{0},{1}]", this.Topic, this.Partiton);
        }

        protected bool Equals(TopicAndPartition other)
        {
            return this.Topic == other.Topic && this.Partiton == other.Partiton;
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

            return Equals((TopicAndPartition)obj);
        }

        public override int GetHashCode()
        {
            return this._hashCode;
        }
    }
}