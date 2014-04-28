namespace Kafka.Client.Common
{
    using System;

    public class TopicAndPartition
    {
        public string Topic { get; private set; }

        public int Partiton { get; private set; }

        public TopicAndPartition(string topic, int partiton)
        {
            this.Topic = topic;
            this.Partiton = partiton;
        }

        public TopicAndPartition(Tuple<string, int> tuple)
            : this(tuple.Item1, tuple.Item2)
        {
        }

        public override string ToString()
        {
            return string.Format("[{0},{1}]", this.Topic, this.Partiton);
        }

        protected bool Equals(TopicAndPartition other)
        {
            return string.Equals(this.Topic, other.Topic) && this.Partiton == other.Partiton;
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
            unchecked
            {
                return ((this.Topic != null ? this.Topic.GetHashCode() : 0) * 397) ^ this.Partiton;
            }
        }
    }
}