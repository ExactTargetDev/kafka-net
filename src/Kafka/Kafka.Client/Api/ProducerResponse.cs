namespace Kafka.Client.Api
{
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    using Kafka.Client.Common;

    using Kafka.Client.Extensions;

    public class ProducerResponse : RequestOrResponse
    {
        public Dictionary<TopicAndPartition, ProducerResponseStatus> Status { get; private set; }

        public ProducerResponse(Dictionary<TopicAndPartition, ProducerResponseStatus> status, int correlationId)
            : base(null, correlationId)
        {
            this.Status = status;
        }

        public static ProducerResponse ReadFrom(MemoryStream buffer)
        {
            var correlationId = buffer.GetInt();
            var topicCount = buffer.GetInt();
            var statusPairs = Enumerable.Range(0, topicCount).SelectMany(
                _ =>
                    {
                        var topic = ApiUtils.ReadShortString(buffer);
                        var partitionCount = buffer.GetInt();
                        return Enumerable.Range(0, partitionCount).Select(
                            __ =>
                                {
                                    var partition = buffer.GetInt();
                                    var error = buffer.GetShort();
                                    var offset = buffer.GetLong();
                                    return new KeyValuePair<TopicAndPartition, ProducerResponseStatus>(
                                        new TopicAndPartition(topic, partition), new ProducerResponseStatus(error, offset));
                                });
                    });

            return new ProducerResponse(statusPairs.ToDictionary(x => x.Key, x => x.Value), correlationId);
        }

        public override string Describe(bool details)
        {
            return this.ToString();
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

    }
}