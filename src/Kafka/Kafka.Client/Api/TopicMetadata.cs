namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    using Kafka.Client.Cluster;
    using Kafka.Client.Serializers;

    using System.Linq;

    public class TopicMetadata
    {
        public const int NoLeaderNodeId = -1;

        internal static TopicMetadata ReadFrom(MemoryStream buffer, Dictionary<int, Broker> brokers)
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

        public TopicMetadata(string topic, List<PartitionMetadata> partitionsMetadata, short errorCode)
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
                    + ApiUtils.ShortStringLength(Topic) + 4
                       + PartitionsMetadata.Aggregate(0, (i, metadata) => i + metadata.SizeInBytes);
                    /* size and partition data array */

            }
        }

        //TODO: writeTo

        //TODO: toString

    }
}