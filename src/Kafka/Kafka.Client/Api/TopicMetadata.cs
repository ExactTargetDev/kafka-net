namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;

    using Kafka.Client.Clusters;
    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Extensions;

    public class TopicMetadata
    {
        public const int NoLeaderNodeId = -1;

        internal static TopicMetadata ReadFrom(ByteBuffer buffer, Dictionary<int, Broker> brokers)
        {
            var errorCode = ApiUtils.ReadShortInRange(buffer, "error code", Tuple.Create((short)-1, short.MaxValue));
            var topic = ApiUtils.ReadShortString(buffer);
            var numPartitions = ApiUtils.ReadIntInRange(buffer, "number of partitions", Tuple.Create(0, int.MaxValue));

            var partitionsMetadata = new List<PartitionMetadata>();
            for (var i = 0; i < numPartitions; i++)
            {
                partitionsMetadata.Add(PartitionMetadata.ReadFrom(buffer, brokers));
            }

            return new TopicMetadata(topic, partitionsMetadata, errorCode);
        }

        public string Topic { get; private set; }

        public List<PartitionMetadata> PartitionsMetadata { get; private set; }

        public short ErrorCode { get; private set; }

        public TopicMetadata(string topic, List<PartitionMetadata> partitionsMetadata, short errorCode = ErrorMapping.NoError)
        {
            this.Topic = topic;
            this.PartitionsMetadata = partitionsMetadata;
            this.ErrorCode = errorCode;
        }

        public int SizeInBytes
        {
            get
            {
                return 2 /* error code */ 
                    + ApiUtils.ShortStringLength(this.Topic) + 4
                       + this.PartitionsMetadata.Aggregate(0, (i, metadata) => i + metadata.SizeInBytes);
                    /* size and partition Data array */
            }
        }

        public void WriteTo(ByteBuffer buffer)
        {
             /* error code */
            buffer.PutShort(this.ErrorCode);
            /* topic */
            ApiUtils.WriteShortString(buffer, this.Topic);
            /* number of partitions */
            buffer.PutInt(this.PartitionsMetadata.Count());
            foreach (var m in this.PartitionsMetadata)
            {
                m.WriteTo(buffer);
            }
        }

        public override string ToString()
        {
            var topicMetadataInfo = new StringBuilder();
            topicMetadataInfo.AppendFormat("[TopicMetadata for topic {0} -> ", this.Topic);
            switch (this.ErrorCode)
            {
                case ErrorMapping.NoError:
                    this.PartitionsMetadata.ForEach(partitionMetadata =>
                        {
                            switch (partitionMetadata.ErrorCode)
                            {
                                case ErrorMapping.NoError:
                                    topicMetadataInfo.AppendFormat(
                                        " Metadata for partition [{0},{1}] is {2}",
                                        this.Topic,
                                        partitionMetadata.PartitionId,
                                        partitionMetadata.ToString());
                                    break;
                                case ErrorMapping.ReplicaNotAvailableCode:
                                    // this error message means some replica other than the leader is not available. The consumer
                                    // doesn't care about non leader replicas, so ignore this
                                    topicMetadataInfo.AppendFormat(
                                        " Metadata for partition [{0},{1}] is {2}",
                                        this.Topic,
                                        partitionMetadata.PartitionId,
                                        partitionMetadata.ToString());
                                    break;
                                default:
                                    topicMetadataInfo.AppendFormat(
                                        " Metadata for partition [{0},{1}] is not available due to {2}",
                                        this.Topic,
                                        partitionMetadata.PartitionId,
                                        ErrorMapping.ExceptionFor(partitionMetadata.ErrorCode).GetType().Name);
                                    break;
                            }
                        });
                    break;
                default:
                    topicMetadataInfo.AppendFormat(
                        "No partiton metadata for topic {0} due to {1}",
                        this.Topic,
                        ErrorMapping.ExceptionFor(this.ErrorCode).GetType().Name);
                    break;
            }

            topicMetadataInfo.Append("]");
            return topicMetadataInfo.ToString();
        }
    }
}