namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Text;

    using Kafka.Client.Clusters;
    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;

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

    public class PartitionMetadata
    {
        public static PartitionMetadata ReadFrom(ByteBuffer buffer, Dictionary<int, Broker> brokers)
        {
            var errorCode = ApiUtils.ReadShortInRange(
                buffer, "error code", Tuple.Create<short, short>(-1, short.MaxValue));
            var partitionId = ApiUtils.ReadIntInRange(buffer, "partition id", Tuple.Create(0, int.MaxValue)); // partition id
            var leaderId = buffer.GetInt();
            var leader = brokers[leaderId];

            // list of all replicas
            var numReplicas = ApiUtils.ReadIntInRange(buffer, "number of all replicas", Tuple.Create(0, int.MaxValue));
            var replicaIds = Enumerable.Range(0, numReplicas).Select(_ => buffer.GetInt()).ToList();
            var replicas = replicaIds.Select(x => brokers[x]).ToList();

            // list of in-sync replicasd
            var numIsr = ApiUtils.ReadIntInRange(buffer, "number of in-sync replicas", Tuple.Create(0, int.MaxValue));
            var isrIds = Enumerable.Range(0, numIsr).Select(_ => buffer.GetInt()).ToList();
            var isr = isrIds.Select(x => brokers[x]).ToList();

            return new PartitionMetadata(partitionId, leader, replicas, isr, errorCode);
        }

        public int PartitionId { get; private set; }

        internal Broker Leader { get; private set; }

        internal IEnumerable<Broker> Replicas { get; private set; }

        internal IEnumerable<Broker> Isr { get; private set; }

        public short ErrorCode { get; private set; }

        public PartitionMetadata(int partitionId, Broker leader, IEnumerable<Broker> replicas, IEnumerable<Broker> isr = null, short errorCode = ErrorMapping.NoError)
        {
            if (isr == null)
            {
                isr = Enumerable.Empty<Broker>();
            }

            this.PartitionId = partitionId;
            this.Leader = leader;
            this.Replicas = replicas;
            this.Isr = isr;
            this.ErrorCode = errorCode;
        }

        public int SizeInBytes
        {
            [SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1407:ArithmeticExpressionsMustDeclarePrecedence", Justification = "Reviewed. Suppression is OK here.")]
            get
            {
                return 2 + /* error code */
                    4 /* partition id */ +
                    4 /* leader */ +
                    4 +
                    4 * this.Replicas.Count() /* replica array */ +
                    4 +
                    4 * this.Isr.Count(); /* isr array */
            }
        }

        public void WriteTo(ByteBuffer buffer)
        {
            buffer.PutShort(this.ErrorCode);
            buffer.PutInt(this.PartitionId);

            // leader
            var leaderId = (this.Leader != null) ? this.Leader.Id : TopicMetadata.NoLeaderNodeId;
            buffer.PutInt(leaderId);

            /* number of replicas */
            buffer.PutInt(this.Replicas.Count());
            foreach (var replica in this.Replicas)
            {
                buffer.PutInt(replica.Id);
            }

            /* number of in-sync replicas */
            buffer.PutInt(this.Isr.Count());
            foreach (var r in this.Isr)
            {
                buffer.PutInt(r.Id);
            }
        }

        public override string ToString()
        {
            var partitionMetadataString = new StringBuilder();
            partitionMetadataString.Append("partition: " + this.PartitionId);
            partitionMetadataString.Append(" leader: " + ((this.Leader != null) ? this.FormatBroker(this.Leader) : "None"));
            partitionMetadataString.Append(
                " replicas: " + string.Join(", ", this.Replicas.Select(this.FormatBroker)));
            partitionMetadataString.Append(" isr: " + string.Join(", ", this.Isr.Select(this.FormatBroker)));
            partitionMetadataString.Append(" isUnderReplicated " + (this.Isr.Count() < this.Replicas.Count() ? "true" : "false"));
            return partitionMetadataString.ToString();
        }

        private string FormatBroker(Broker broker)
        {
            return string.Format("{0} ({1}:{2})", broker.Id, broker.Host, broker.Port);
        }
    }
}