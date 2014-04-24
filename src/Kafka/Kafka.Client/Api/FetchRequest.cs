using System;

namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    using Kafka.Client.Cfg;
    using Kafka.Client.Common;
    using Kafka.Client.Extensions;
    using Kafka.Client.ZKClient.Exceptions;

    using System.Linq;

    public class FetchRequest : RequestOrResponse
    {
        public const short CurrentVersion = 0;

        public const int DefaultMaxWait = 0;

        public const int DefaultMinBytes = 0;

        public const int DefaultCorrelationId = 0;

        public static FetchRequest ReadFrom(MemoryStream buffer)
        {
            throw new NotImplementedException();
        }

        public short VersionId { get; private set; }

        public string ClientId { get; private set; }

        public int ReplicaId { get; private set; }

        public int MaxWait { get; private set; }

        public int MinBytes { get; private set; }

        public IDictionary<TopicAndPartition, PartitionFetchInfo> RequestInfo { get; private set; }

        private readonly Lazy<IDictionary<string, IDictionary<TopicAndPartition, PartitionFetchInfo>>>
            requestInfoGroupedByTopic;

        internal FetchRequest(short versionId = CurrentVersion, int correlationId = DefaultCorrelationId, string clientId = ConsumerConfiguration.DefaultClientId, int replicaId = Request.OrdinaryConsumerId, int maxWait = DefaultMaxWait, int minBytes = DefaultMinBytes, IDictionary<TopicAndPartition, PartitionFetchInfo> requestInfo = null)
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

        public override void WriteTo(MemoryStream buffer)
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



        /*TODO  def isFromFollower = Request.isReplicaIdFromFollower(replicaId) */

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

        public override void HandleError(Exception e, Network.RequestChannel requestChannel, Network.RequestChannelRequest request)
        {
            throw new NotImplementedException();
        }

        public override string Describe(bool details)
        {
            throw new NotImplementedException();
        }

    }
}