namespace Kafka.Client.Api
{
    using System.Collections.Generic;
    using System.IO;

    using Kafka.Client.Common;

    public class ProducerResponse : RequestOrResponse
    {
        public Dictionary<TopicAndPartition, ProducerResponseStatus> Status { get; private set; }

        public ProducerResponse(Dictionary<TopicAndPartition, ProducerResponseStatus> status, int correlationId)
            : base(null, correlationId)
        {
            this.Status = status;
        }

        //TODO: finish me!

        public override string Describe(bool details)
        {
            throw new System.NotImplementedException();
        }

        public override int SizeInBytes
        {
            get
            {
                throw new System.NotImplementedException();
            }
        }

        public override void WriteTo(MemoryStream bufffer)
        {
            throw new System.NotImplementedException();
        }

        //TODO: 
    }
}