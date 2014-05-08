namespace Kafka.Client.Consumers
{
    using System;

    using Kafka.Client.Messages;

    public class FetchedDataChunk : IEquatable<FetchedDataChunk>
    {
        public ByteBufferMessageSet Messages { get; set; }

        public PartitionTopicInfo TopicInfo { get; set; }

        public long FetchOffset { get; set; }

        public FetchedDataChunk(ByteBufferMessageSet messages, PartitionTopicInfo topicInfo, long fetchOffset)
        {
            this.Messages = messages;
            this.TopicInfo = topicInfo;
            this.FetchOffset = fetchOffset;
        }

        public bool Equals(FetchedDataChunk other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            return Equals(this.Messages, other.Messages) && Equals(this.TopicInfo, other.TopicInfo) && this.FetchOffset == other.FetchOffset;
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

            return obj.GetType() == this.GetType() && this.Equals((FetchedDataChunk)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = this.Messages != null ? this.Messages.GetHashCode() : 0;
                hashCode = (hashCode * 397) ^ (this.TopicInfo != null ? this.TopicInfo.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ this.FetchOffset.GetHashCode();
                return hashCode;
            }
        }
    }
}