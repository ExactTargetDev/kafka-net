namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    using Kafka.Client.Clusters;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Extensions;

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
                throw new NotSupportedException();
            }
        }

        public override void WriteTo(ByteBuffer buffer)
        {
            throw new NotSupportedException();
        }

        public override string Describe(bool details)
        {
            return this.ToString();
        }

        public override string ToString()
        {
            return string.Format("TopicMetadataResponse(TopicsMetadata: {0}, SizeInBytes: {1})", string.Join("," , this.TopicsMetadata), this.SizeInBytes);
        }
    }
}