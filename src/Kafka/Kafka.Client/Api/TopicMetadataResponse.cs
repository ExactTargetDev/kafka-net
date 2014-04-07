namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    public class TopicMetadataResponse : RequestOrResponse
    {

        public static TopicMetadataResponse ReadFrom(MemoryStream buffer)
        {
            throw new NotImplementedException();
        }

        public List<TopicMetadata> TopicsMetadata { get; private set; }

        public TopicMetadataResponse(List<TopicMetadata> topicsMetadata , int correlationId)
            : base(null, correlationId)
        {
            this.TopicsMetadata = topicsMetadata;
        }

        public override string Describe(bool details)
        {
            return this.ToString();
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

        //TODO: 
    }
}