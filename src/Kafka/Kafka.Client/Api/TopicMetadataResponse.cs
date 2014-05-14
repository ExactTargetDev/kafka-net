namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using Kafka.Client.Clusters;
    using Kafka.Client.Common.Imported;

    public class TopicMetadataResponse : RequestOrResponse
    {
        public static TopicMetadataResponse ReadFrom(ByteBuffer buffer)
        {
            var correlationId = buffer.GetInt();
            var brokerCount = buffer.GetInt();
            var brokers = Enumerable.Range(0, brokerCount).Select(_ => Broker.ReadFrom(buffer)).ToList();
            var brokerMap = brokers.ToDictionary(b => b.Id);
            var topicCount = buffer.GetInt();
            var topicsMetadata =
                Enumerable.Range(0, topicCount).Select(_ => TopicMetadata.ReadFrom(buffer, brokerMap)).ToList();
            return new TopicMetadataResponse(topicsMetadata, correlationId);
        }

        public List<TopicMetadata> TopicsMetadata { get; private set; }

        public TopicMetadataResponse(List<TopicMetadata> topicsMetadata, int correlationId)
            : base(null, correlationId)
        {
            this.TopicsMetadata = topicsMetadata;
        }

        public override int SizeInBytes
        {
            get
            {
                var brokers = this.ExtractBrokers(this.TopicsMetadata).Values;
                return 4 + 4 + brokers.Sum(b => b.SizeInBytes) + 4 + this.TopicsMetadata.Sum(x => x.SizeInBytes);
            }
        }

        public override void WriteTo(ByteBuffer buffer)
        {
            buffer.PutInt(this.CorrelationId);

            /* brokers */
            var brokers = this.ExtractBrokers(this.TopicsMetadata).Values;
            buffer.PutInt(brokers.Count);
            foreach (var broker in brokers)
            {
                broker.WriteTo(buffer);
            }

            /* topic metadata */
            buffer.PutInt(this.TopicsMetadata.Count);
            foreach (var topicMeta in this.TopicsMetadata)
            {
                topicMeta.WriteTo(buffer);
            }
        }

        public IDictionary<int, Broker> ExtractBrokers(List<TopicMetadata> topicMetadatas)
        {
            var parts = topicMetadatas.SelectMany(x => x.PartitionsMetadata).ToList();
            var brokers = parts.SelectMany(x => x.Replicas).Union(parts.Select(x => x.Leader));
            return brokers.ToDictionary(x => x.Id, x => x);
        }

        public override string Describe(bool details)
        {
            return this.ToString();
        }

        public override string ToString()
        {
            return string.Format("TopicMetadataResponse(TopicsMetadata: {0}, SizeInBytes: {1})", string.Join(",", this.TopicsMetadata), this.SizeInBytes);
        }

        protected bool Equals(TopicMetadataResponse other)
        {
            return this.CorrelationId == other.CorrelationId && this.TopicsMetadata.SequenceEqual(other.TopicsMetadata);
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
            return Equals((TopicMetadataResponse)obj);
        }

        public override int GetHashCode()
        {
            throw new NotSupportedException();
        }
    }
}