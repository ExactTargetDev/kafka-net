namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    using Kafka.Client.Cluster;
    using Kafka.Client.Extensions;

    public class TopicMetadataResponse : RequestOrResponse
    {
        public static TopicMetadataResponse ReadFrom(MemoryStream buffer)
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
                throw new NotImplementedException();
            }
        }

        public override void WriteTo(MemoryStream bufffer)
        {
            throw new NotImplementedException();
        }

        public Dictionary<int, Broker> ExtractBrokers(List<TopicMetadata> topicMetadatas)
        {
            throw new NotImplementedException();
        }

        public override string Describe(bool details)
        {
            return this.ToString();
        }
    }
}