using System;

namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;

    using Kafka.Client.Cfg;
    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Extensions;
    using Kafka.Client.Messages;
    using Kafka.Client.Network;
    using Kafka.Client.ZKClient.Exceptions;

    using System.Linq;

    public class FetchRequest : RequestOrResponse
    {
        public const short CurrentVersion = 0;

        public const int DefaultMaxWait = 0;

        public const int DefaultMinBytes = 0;

        public const int DefaultCorrelationId = 0;

        public static FetchRequest ReadFrom(ByteBuffer buffer)
        {
            throw new NotSupportedException();
        }

        public short VersionId { get; private set; }

        public string ClientId { get; private set; }

        public int ReplicaId { get; private set; }

        public int MaxWait { get; private set; }

        public int MinBytes { get; private set; }

        public IDictionary<TopicAndPartition, PartitionFetchInfo> RequestInfo { get; private set; }

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

        public FetchRequest(int correlationId, string clientId, int maxWait, int minBytes, IDictionary<TopicAndPartition, PartitionFetchInfo> requestInfo)
            : this(FetchRequest.CurrentVersion, correlationId, clientId, Request.OrdinaryConsumerId, maxWait, minBytes, requestInfo)
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
            get
            {
                return 2 + /* versionId */
                4 + /* correlationId */
                ApiUtils.ShortStringLength(this.ClientId) +
                4 + /* replicaId */
                4 + /* maxWait */
                4 + /* minBytes */
                4 + /* topic count */
                this.requestInfoGroupedByTopic.Value.Aggregate(0, (foldedTopics, currTopic) =>
                    {
                        var topic = currTopic.Key;
                        var partitionFetchInfos = currTopic.Value;
                        return foldedTopics +
                            ApiUtils.ShortStringLength(topic) + 
                            4 + /* partition count */ + 
                            partitionFetchInfos.Count * (4 + /* partition id */
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
            fetchRequest.Append("; Version: " + VersionId);
            fetchRequest.Append("; CorrelationId: " + CorrelationId);
            fetchRequest.Append("; ClientId: " + ClientId);
            fetchRequest.Append("; ReplicaId: " + ReplicaId);
            fetchRequest.Append("; MaxWait: " + MaxWait + " ms");
            fetchRequest.Append("; MinBytes: " + MinBytes + " bytes");
            if (details)
            {
                fetchRequest.Append("; RequestInfo: " + string.Join(",", RequestInfo));
            }

            return fetchRequest.ToString();
        }

    }

    public class FetchRequestBuilder
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
                versionId,
                correlationId.GetAndIncrement(),
                clientId,
                replicaId,
                maxWait,
                minBytes,
                new Dictionary<TopicAndPartition, PartitionFetchInfo>(requestMap));

            this.requestMap.Clear();
            return fetchRequest;
        }

    }
}