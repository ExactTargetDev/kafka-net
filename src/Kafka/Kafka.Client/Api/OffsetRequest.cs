namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;

    using Kafka.Client.Common;

    using Kafka.Client.Extensions;

    using System.Linq;

    internal class OffsetRequest : RequestOrResponse
    {
        public const short CurrentVersion = 0;

        public const string DefaultClientId = "";

        public const string SmallestTimeString = "smallest";
        public const string LargestTimeString = "largest";

        public const long LatestTime = -1;
        public const long EarliestTime = -2;

        public static OffsetRequest ReadFrom(MemoryStream buffer)
        {
            throw new NotImplementedException();
        }

        public IDictionary<TopicAndPartition, PartitionOffsetRequestInfo> RequestInfo { get; private set; }

        public short VersionId { get; private set; }

        public string ClientId { get; private set; }

        public int ReplicaId { get; private set; }

        private readonly Lazy<IDictionary<string, IDictionary<TopicAndPartition, PartitionOffsetRequestInfo>>>
            requestInfoGroupedByTopic;

        public OffsetRequest(
            IDictionary<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo,
            short versionId = OffsetRequest.CurrentVersion,
            int correlationId = 0,
            string clientId = OffsetRequest.DefaultClientId,
            int replicaId = Request.OrdinaryConsumerId) : base (RequestKeys.OffsetsKey, correlationId)
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
            : this(requestInfo, OffsetRequest.CurrentVersion, correlationId, OffsetRequest.DefaultClientId, replicaId)
        {
        }

        public override void WriteTo(MemoryStream buffer)
        {
            buffer.PutShort(this.VersionId);
            buffer.PutInt(this.CorrelationId);
            ApiUtils.WriteShortString(buffer, this.ClientId);
            buffer.PutInt(this.ReplicaId);

            buffer.PutInt(requestInfoGroupedByTopic.Value.Count);
            foreach (var topicAndPartitionInfos in requestInfoGroupedByTopic.Value)
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
            get 
            {
                return 2 + /* versionId */
                    4 + /* correlationId */
                    ApiUtils.ShortStringLength(ClientId) +
                    4 + /* replicaId */
                    4 + /* topic count */
                    requestInfoGroupedByTopic.Value.Aggregate(0, (foldedTopics, currTopic) =>
                        {
                            var topic = currTopic.Key;
                            var partitionInfos = currTopic.Value;
                            return foldedTopics +
                                ApiUtils.ShortStringLength(topic) +
                                 4 + /* partition count */
                                 partitionInfos.Count *
                                 (
                                 4 + /* partition */
                                    8 + /* time */
                                    4 /* maxNumOffsets */
                                 );
                        });
            }
        }

        public bool IsFromOrdinaryClient 
        {
            get
            {
                return ReplicaId == Request.OrdinaryConsumerId;
            }
        }

        public override string ToString()
        {
            return this.Describe(true);
        }

        public override void HandleError(Exception e, Network.RequestChannel requestChannel, Network.RequestChannelRequest request)
        {
            throw new NotImplementedException();
        }

        public override string Describe(bool details)
        {
            var offsetRequest = new StringBuilder();
            offsetRequest.Append("Name: " + this.GetType().Name);
            offsetRequest.Append("; Version: " + VersionId);
            offsetRequest.Append("; CorrelationId: " + CorrelationId);
            offsetRequest.Append("; ClientId: " + ClientId);
            offsetRequest.Append("; ReplicaId: " + ReplicaId);
            if (details)
            {
                offsetRequest.Append("; RequestInfo: " + string.Join(",", RequestInfo)); //TODO: better formatting? 
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
            return Equals((PartitionOffsetRequestInfo)obj);
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