namespace Kafka.Client.Api
{
    using System;
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

            this.statusGroupedByTopic = new Lazy<IDictionary<string, IDictionary<TopicAndPartition, ProducerResponseStatus>>>(() =>
                this.Status.GroupByScala(x => x.Key.Topic));
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

        private readonly Lazy<IDictionary<string, IDictionary<TopicAndPartition, ProducerResponseStatus>>> statusGroupedByTopic;

        public bool HasError()
        {
            return this.Status.Values.Any(v => v.Error != ErrorMapping.NoError);
        }

        public override int SizeInBytes
        {
            get
            {
                var groupedStatus = this.statusGroupedByTopic.Value;
                return 4 + /* correlation id */ 
                    4 + /* topic count */ 
                    groupedStatus.Aggregate(
                           0,
                           (foldedTopics, currTopic) =>
                               {
                                   return foldedTopics + 
                                       ApiUtils.ShortStringLength(currTopic.Key) + 
                                       4 + /* partition count for this topic */
                                       currTopic.Value.Count * 
                                       (4 + /* partition id */ 2 + /* error code */ 8 /* offset */);
                               });
            }
        }

        public override void WriteTo(MemoryStream bufffer)
        {
            throw new NotImplementedException("Not used in client");
        }

        public override string Describe(bool details)
        {
            return this.ToString();
        }
    }
}