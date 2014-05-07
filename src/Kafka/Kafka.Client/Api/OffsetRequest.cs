namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Text;

    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Extensions;

    internal class OffsetRequest : RequestOrResponse
    {
        public const short CurrentVersion = 0;

        public const string DefaultClientId = "";

        public const string SmallestTimeString = "smallest";

        public const string LargestTimeString = "largest";

        public const long LatestTime = -1;
        public const long EarliestTime = -2;

        public static OffsetRequest ReadFrom(ByteBuffer buffer)
        {
            throw new NotSupportedException();
        }

        public IDictionary<TopicAndPartition, PartitionOffsetRequestInfo> RequestInfo { get; private set; }

        public short VersionId { get; private set; }

        public string ClientId { get; private set; }

        public int ReplicaId { get; private set; }

        private readonly Lazy<IDictionary<string, IDictionary<TopicAndPartition, PartitionOffsetRequestInfo>>>
            requestInfoGroupedByTopic;

        public OffsetRequest(
            IDictionary<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo,
            short versionId = CurrentVersion,
            int correlationId = 0,
            string clientId = DefaultClientId,
            int replicaId = Request.OrdinaryConsumerId) : base(RequestKeys.OffsetsKey, correlationId)
        {
            this.RequestInfo = requestInfo;
            this.VersionId = versionId;
            this.ClientId = clientId;
            this.ReplicaId = replicaId;
            this.requestInfoGroupedByTopic = new Lazy<IDictionary<string, IDictionary<TopicAndPartition, PartitionOffsetRequestInfo>>>(
                () => this.RequestInfo.GroupByScala(r => r.Key.Topic));
        }

        public OffsetRequest(
            IDictionary<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo, int correlationId, int replicaId)
            : this(requestInfo, CurrentVersion, correlationId, DefaultClientId, replicaId)
        {
        }

        public override void WriteTo(ByteBuffer buffer)
        {
            buffer.PutShort(this.VersionId);
            buffer.PutInt(this.CorrelationId);
            ApiUtils.WriteShortString(buffer, this.ClientId);
            buffer.PutInt(this.ReplicaId);

            buffer.PutInt(this.requestInfoGroupedByTopic.Value.Count);
            foreach (var topicAndPartitionInfos in this.requestInfoGroupedByTopic.Value)
            {
                var topic = topicAndPartitionInfos.Key;
                var partitionInfos = topicAndPartitionInfos.Value;
                ApiUtils.WriteShortString(buffer, topic);
                buffer.PutInt(partitionInfos.Count); // partition count
                foreach (var pi in partitionInfos)
                {
                    var partition = pi.Key.Partiton;
                    var partitionInfo = pi.Value;
                    buffer.PutInt(partition);
                    buffer.PutLong(partitionInfo.Time);
                    buffer.PutInt(partitionInfo.MaxNumOffsets);
                }
            }
        }

        public override int SizeInBytes
        {
            [SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1407:ArithmeticExpressionsMustDeclarePrecedence", Justification = "Reviewed. Suppression is OK here.")]
            get 
            {
                return 2 + /* versionId */
                    4 + /* correlationId */
                    ApiUtils.ShortStringLength(this.ClientId) +
                    4 + /* replicaId */
                    4 + /* topic count */
                    this.requestInfoGroupedByTopic.Value.Aggregate(
                    0, 
                    (foldedTopics, currTopic) =>
                        {
                            var topic = currTopic.Key;
                            var partitionInfos = currTopic.Value;
                            return foldedTopics +
                                ApiUtils.ShortStringLength(topic) +
                                 4 + /* partition count */
                                 partitionInfos.Count *
                                 (4 + /* partition */
                                    8 + /* time */
                                    4 /* maxNumOffsets */);
                        });
            }
        }

        public bool IsFromOrdinaryClient 
        {
            get
            {
                return this.ReplicaId == Request.OrdinaryConsumerId;
            }
        }

        public override string ToString()
        {
            return this.Describe(true);
        }

        public override string Describe(bool details)
        {
            var offsetRequest = new StringBuilder();
            offsetRequest.Append("Name: " + this.GetType().Name);
            offsetRequest.Append("; Version: " + this.VersionId);
            offsetRequest.Append("; CorrelationId: " + this.CorrelationId);
            offsetRequest.Append("; ClientId: " + this.ClientId);
            offsetRequest.Append("; ReplicaId: " + this.ReplicaId);
            if (details)
            {
                offsetRequest.Append("; RequestInfo: " + this.RequestInfo.DictionaryToString());
            }

            return offsetRequest.ToString();
        }
    }

    internal class PartitionOffsetRequestInfo
    {
        public long Time { get; private set; }

        public int MaxNumOffsets { get; private set; }

        public PartitionOffsetRequestInfo(long time, int maxNumOffsets)
        {
            this.Time = time;
            this.MaxNumOffsets = maxNumOffsets;
        }

        public override string ToString()
        {
            return string.Format("Time: {0}, MaxNumOffsets: {1}", this.Time, this.MaxNumOffsets);
        }

        protected bool Equals(PartitionOffsetRequestInfo other)
        {
            return this.Time == other.Time && this.MaxNumOffsets == other.MaxNumOffsets;
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

            return this.Equals((PartitionOffsetRequestInfo)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (this.Time.GetHashCode() * 397) ^ this.MaxNumOffsets;
            }
        }
    }
}