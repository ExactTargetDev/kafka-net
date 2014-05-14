namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Text;

    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Consumers;
    using Kafka.Client.Extensions;

    internal class PartitionFetchInfo
    {
        public long Offset { get; private set; }

        public int FetchSize { get; private set; }

        public PartitionFetchInfo(long offset, int fetchSize)
        {
            this.Offset = offset;
            this.FetchSize = fetchSize;
        }

        protected bool Equals(PartitionFetchInfo other)
        {
            return this.Offset == other.Offset && this.FetchSize == other.FetchSize;
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

            return this.Equals((PartitionFetchInfo)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (this.Offset.GetHashCode() * 397) ^ this.FetchSize;
            }
        }

        public override string ToString()
        {
            return string.Format("PartitionFetchInfo(Offset: {0}, FetchSize: {1})", this.Offset, this.FetchSize);
        }
    }

    internal class FetchRequest : RequestOrResponse
    {
        public const short CurrentVersion = 0;

        public const int DefaultMaxWait = 0;

        public const int DefaultMinBytes = 0;

        public const int DefaultCorrelationId = 0;

        public static FetchRequest ReadFrom(ByteBuffer buffer)
        {
            var versionId = buffer.GetShort();
            var correlationId = buffer.GetInt();
            var clientId = ApiUtils.ReadShortString(buffer);
            var replicaId = buffer.GetInt();
            var maxWait = buffer.GetInt();
            var minBytes = buffer.GetInt();
            var topicCount = buffer.GetInt();
            var pairs = Enumerable.Range(1, topicCount).SelectMany(_ =>
                {
                    var topic = ApiUtils.ReadShortString(buffer);
                    var partitionCount = buffer.GetInt();
                    return Enumerable.Range(1, partitionCount).Select(__ =>
                        {
                            var partitionId = buffer.GetInt();
                            var offset = buffer.GetLong();
                            var fetchSize = buffer.GetInt();
                            return Tuple.Create(
                                new TopicAndPartition(topic, partitionId), new PartitionFetchInfo(offset, fetchSize));
                        });
                });
            return new FetchRequest(versionId, correlationId, clientId, replicaId, maxWait, minBytes, pairs.ToDictionary(x => x.Item1, x => x.Item2));
        }

        public short VersionId { get; private set; }

        public string ClientId { get; private set; }

        public int ReplicaId { get; private set; }

        public int MaxWait { get; private set; }

        public int MinBytes { get; private set; }

        public IDictionary<TopicAndPartition, PartitionFetchInfo> RequestInfo { get; private set; }

        /// <summary>
        /// Partitions the request info into a map of maps (one for each topic).
        /// </summary>
        private readonly Lazy<IDictionary<string, IDictionary<TopicAndPartition, PartitionFetchInfo>>>
            requestInfoGroupedByTopic;

        internal FetchRequest(short versionId = CurrentVersion, int correlationId = DefaultCorrelationId, string clientId = ConsumerConfig.DefaultClientId, int replicaId = Request.OrdinaryConsumerId, int maxWait = DefaultMaxWait, int minBytes = DefaultMinBytes, IDictionary<TopicAndPartition, PartitionFetchInfo> requestInfo = null)
            : base(RequestKeys.FetchKey, correlationId)
        {
            this.VersionId = versionId;
            this.ClientId = clientId;
            this.ReplicaId = replicaId;
            this.MaxWait = maxWait;
            this.MinBytes = minBytes;
            this.RequestInfo = requestInfo;

            this.requestInfoGroupedByTopic = new Lazy<IDictionary<string, IDictionary<TopicAndPartition, PartitionFetchInfo>>>(
                () => this.RequestInfo.GroupByScala(kvp => kvp.Key.Topic));
        }

        /// <summary>
        /// Public constructor for the clients
        /// </summary>
        /// <param name="correlationId"></param>
        /// <param name="clientId"></param>
        /// <param name="maxWait"></param>
        /// <param name="minBytes"></param>
        /// <param name="requestInfo"></param>
        public FetchRequest(int correlationId, string clientId, int maxWait, int minBytes, IDictionary<TopicAndPartition, PartitionFetchInfo> requestInfo)
            : this(CurrentVersion, correlationId, clientId, Request.OrdinaryConsumerId, maxWait, minBytes, requestInfo)
        {
        }

        public override void WriteTo(ByteBuffer buffer)
        {
            buffer.PutShort(this.VersionId);
            buffer.PutInt(this.CorrelationId);
            ApiUtils.WriteShortString(buffer, this.ClientId);
            buffer.PutInt(this.ReplicaId);
            buffer.PutInt(this.MaxWait);
            buffer.PutInt(this.MinBytes);
            buffer.PutInt(this.requestInfoGroupedByTopic.Value.Count); // topic count
            foreach (var kvp in this.requestInfoGroupedByTopic.Value)
            {
                var topic = kvp.Key;
                var partitionFetchInfos = kvp.Value;
                ApiUtils.WriteShortString(buffer, topic);
                buffer.PutInt(partitionFetchInfos.Count); // partition count
                foreach (var pfi in partitionFetchInfos)
                {
                    buffer.PutInt(pfi.Key.Partiton);
                    buffer.PutLong(pfi.Value.Offset);
                    buffer.PutInt(pfi.Value.FetchSize);
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
                4 + /* maxWait */
                4 + /* minBytes */
                4 + /* topic count */
                this.requestInfoGroupedByTopic.Value.Aggregate(
                    0, 
                    (foldedTopics, currTopic) =>
                    {
                        var topic = currTopic.Key;
                        var partitionFetchInfos = currTopic.Value;
                        return foldedTopics +
                            ApiUtils.ShortStringLength(topic) + 
                            4 + /* partition count */ +partitionFetchInfos.Count * (4 + /* partition id */
                            8 + /* offset */
                            4 /* fetch size */);
                    });
            }
        }

        public bool IsFromFailover
        {
            get
            {
                return Request.IsReplicaIdFromFollower(this.ReplicaId);
            }
        }

        public bool FromOrdinaryConsumer
        {
            get
            {
                return this.ReplicaId == Request.OrdinaryConsumerId;
            }
        }

        public bool IsFromLowLevelConsumer
        {
            get
            {
                return this.ReplicaId == Request.DebuggingConsumerId;
            }
        }

        public int NumPartitions
        {
            get
            {
                return this.RequestInfo.Count;
            }
        }

        public override string ToString()
        {
            return this.Describe(true);
        }

        public override string Describe(bool details)
        {
            var fetchRequest = new StringBuilder();
            fetchRequest.Append("Name: " + this.GetType().Name);
            fetchRequest.Append("; Version: " + this.VersionId);
            fetchRequest.Append("; CorrelationId: " + this.CorrelationId);
            fetchRequest.Append("; ClientId: " + this.ClientId);
            fetchRequest.Append("; ReplicaId: " + this.ReplicaId);
            fetchRequest.Append("; MaxWait: " + this.MaxWait + " ms");
            fetchRequest.Append("; MinBytes: " + this.MinBytes + " bytes");
            if (details)
            {
                fetchRequest.Append("; RequestInfo: " + this.RequestInfo.DictionaryToString());
            }

            return fetchRequest.ToString();
        }

        protected bool Equals(FetchRequest other)
        {
            return this.VersionId == other.VersionId && string.Equals(this.ClientId, other.ClientId)
                   && this.ReplicaId == other.ReplicaId && this.CorrelationId == other.CorrelationId
                   && this.MaxWait == other.MaxWait && this.MinBytes == other.MinBytes
                   && this.RequestInfo.DictionaryEqual(other.RequestInfo);
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

            return Equals((FetchRequest)obj);
        }

        public override int GetHashCode()
        {
            throw new NotSupportedException();
        }
    }

    internal class FetchRequestBuilder
    {
        private readonly AtomicInteger correlationId = new AtomicInteger(0);

        private readonly short versionId = FetchRequest.CurrentVersion;

        private string clientId = ConsumerConfig.DefaultClientId;

        private int replicaId = Request.OrdinaryConsumerId;

        private int maxWait = FetchRequest.DefaultMaxWait;

        private int minBytes = FetchRequest.DefaultMinBytes;

        private Dictionary<TopicAndPartition, PartitionFetchInfo> requestMap = new Dictionary<TopicAndPartition, PartitionFetchInfo>();

        public FetchRequestBuilder AddFetch(string topic, int partition, long offset, int fetchSize)
        {
            this.requestMap[new TopicAndPartition(topic, partition)] = new PartitionFetchInfo(offset, fetchSize);
            return this;
        }

        public FetchRequestBuilder ClientId(string clientId)
        {
            this.clientId = clientId;
            return this;
        }

        internal FetchRequestBuilder ReplicaId(int replicaId)
        {
            this.replicaId = replicaId;
            return this;
        }

        internal FetchRequestBuilder MaxWait(int maxWait)
        {
            this.maxWait = maxWait;
            return this;
        }

        internal FetchRequestBuilder MinBytes(int minBytes)
        {
            this.minBytes = minBytes;
            return this;
        }

        public FetchRequest Build()
        {
            var fetchRequest = new FetchRequest(
                this.versionId,
                this.correlationId.GetAndIncrement(),
                this.clientId,
                this.replicaId,
                this.maxWait,
                this.minBytes,
                new Dictionary<TopicAndPartition, PartitionFetchInfo>(this.requestMap));

            this.requestMap.Clear();
            return fetchRequest;
        }
    }
}