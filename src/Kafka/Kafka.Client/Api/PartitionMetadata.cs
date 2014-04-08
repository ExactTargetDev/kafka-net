namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Security.Principal;

    using Kafka.Client.Cluster;
    using Kafka.Client.Common;

    using Kafka.Client.Extensions;

    public class PartitionMetadata
    {
        public int PartitionId { get; private set; }

        internal Broker Leader { get; private set; }

        internal IEnumerable<Broker> Replicas { get; private set; }

        internal IEnumerable<Broker> Isr { get; private set; }

        public short ErrorCode { get; private set; }

        public static PartitionMetadata ReadFrom(MemoryStream buffer, Dictionary<int, Broker> brokers)
        {
            var errorCode = ApiUtils.ReadShortInRange(
                buffer, "error code", Tuple.Create<short, short>(-1, short.MaxValue));
            var partitionId = ApiUtils.ReadIntInRange(buffer, "partition id", Tuple.Create(0, int.MaxValue)); //partition id
            var leaderId = buffer.GetInt();
            var leader = brokers[leaderId];

            // list of all replicas
            var numReplicas = ApiUtils.ReadIntInRange(buffer, "number of all replicas", Tuple.Create(0, int.MaxValue));
            var replicaIds = Enumerable.Range(0, numReplicas).Select(_ => buffer.GetInt()).ToList();
            var replicas = replicaIds.Select(r => brokers[r]).ToList();

            // list of in-sync replicasd
            var numIsr = ApiUtils.ReadIntInRange(buffer, "number of in-sync replicas", Tuple.Create(0, int.MaxValue));
            var isrIds = Enumerable.Range(0, numIsr).Select(_ => buffer.GetInt()).ToList();
            var isr = isrIds.Select(r => brokers[r]).ToList();

            return new PartitionMetadata(partitionId, leader, replicas, isr, errorCode);
        }

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
            get
            {
                return 2 /* error code */ + 4 /* partition id */ + 4 /* leader */+ 4 + 4 * Replicas.Count()
                    /* replica array */+ 4 + 4 * Isr.Count(); /* isr array */;
            }
        }

        //TODO: write to

        //TODO to string

        private string FormatBroker(Broker broker)
        {
            return string.Format("{0} ({1}:{2})", broker.Id, broker.Host, broker.Port);
        }
    }
}