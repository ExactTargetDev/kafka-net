using System;
using Kafka.Client.Messages;

namespace Kafka.Client.Consumers
{
    internal class FetchedDataChunk : IEquatable<FetchedDataChunk>
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

        public override bool Equals(object obj)
        {
            var other = obj as FetchedDataChunk;
            return other != null && this.Equals(other);
        }

        public bool Equals(FetchedDataChunk other)
        {
            return Messages == other.Messages &&
                    TopicInfo == other.TopicInfo &&
                    FetchOffset == other.FetchOffset;
        }
    }
}