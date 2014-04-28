namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;

    using Kafka.Client.Common.Imported;
    using Kafka.Client.Extensions;

    public class TopicMetadataRequest : RequestOrResponse
    {
        public const short CurrentVersion = 0;

        public const string DefaultClientId = "";

        public static TopicMetadataRequest ReadFrom(ByteBuffer buffer)
        {
            throw new NotImplementedException();
        }

        public short VersionId { get; private set; }

        public string ClientId { get; private set; }

        public IList<string> Topics { get; private set; }

        public TopicMetadataRequest(short versionId, int correlationId, string clientId, IList<string> topics)
            : base(RequestKeys.MetadataKey, correlationId)
        {
            this.VersionId = versionId;
            this.ClientId = clientId;
            this.Topics = topics;
        }

        public TopicMetadataRequest(IList<string> topics, int correlationId) : this(CurrentVersion, correlationId, DefaultClientId, topics)
        {
        }

        public override void WriteTo(ByteBuffer buffer)
        {
            buffer.PutShort(this.VersionId);
            buffer.PutInt(this.CorrelationId);
            ApiUtils.WriteShortString(buffer, this.ClientId);
            buffer.PutInt(this.Topics.Count);
            foreach (var topic in this.Topics)
            {
                ApiUtils.WriteShortString(buffer, topic);
            }
        }

        public override int SizeInBytes
        {
            get
            {
                return 2 + /* version id */ 
                       4 + /* correlation id */ ApiUtils.ShortStringLength(this.ClientId)
                       + /* client id */ 4
                       + /* number of topics */ this.Topics.Aggregate(0, (i, s) => i + ApiUtils.ShortStringLength(s));
                    /* topics */
            }
        }

        public override string ToString()
        {
            return this.Describe(true);
        }

        public override string Describe(bool details)
        {
            var topicMetadataRequest = new StringBuilder();
            topicMetadataRequest.Append("Name: " + this.GetType().Name);
            topicMetadataRequest.Append("; Version: " + this.VersionId);
            topicMetadataRequest.Append("; CorrelationId: " + this.CorrelationId);
            topicMetadataRequest.Append("; ClientId: " + this.ClientId);
            if (details)
            {
                topicMetadataRequest.Append("; Topics: " + string.Join(",", this.Topics));
            }
            return topicMetadataRequest.ToString();
        }
    }
}