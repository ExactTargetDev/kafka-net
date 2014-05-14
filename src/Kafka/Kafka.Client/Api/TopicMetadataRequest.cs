namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    using Kafka.Client.Common.Imported;

    public class TopicMetadataRequest : RequestOrResponse
    {
        public const short CurrentVersion = 0;

        public const string DefaultClientId = "";

        public static TopicMetadataRequest ReadFrom(ByteBuffer buffer)
        {
            var versonId = buffer.GetShort();
            var correlationID = buffer.GetInt();
            var clientId = ApiUtils.ReadShortString(buffer);
            var numTopic = ApiUtils.ReadIntInRange(buffer, "number of topics", Tuple.Create(0, int.MaxValue));
            var topics = new List<string>(numTopic);
            for (var i = 0; i < numTopic; i++)
            {
                topics.Add(ApiUtils.ReadShortString(buffer));
            }

            return new TopicMetadataRequest(versonId, correlationID, clientId, topics);
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

        protected bool Equals(TopicMetadataRequest other)
        {
            return this.VersionId == other.VersionId && this.CorrelationId == other.CorrelationId
                   && string.Equals(this.ClientId, other.ClientId) && this.Topics.SequenceEqual(other.Topics);
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
            return Equals((TopicMetadataRequest)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = this.VersionId.GetHashCode();
                hashCode = (hashCode * 397) ^ (this.ClientId != null ? this.ClientId.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (this.Topics != null ? this.Topics.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}