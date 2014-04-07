namespace Kafka.Client.Api
{
    using System.Collections.Generic;
    using System.Linq;

    using Kafka.Client.Cluster;
    using Kafka.Client.Common;

    public class PartitionMetadata
    {
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
            get
            {
                return 2 /* error code */+ 4 /* partition id */+ 4 /* leader */+ 4 + 4 * Replicas.Count()
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