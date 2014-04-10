namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;

    using Kafka.Client.Common;
    using Kafka.Client.Extensions;
    using Kafka.Client.Messages;

    internal class ProducerRequest : RequestOrResponse
    {
        public const short CurrentVersion = 0;

        public short VersionId { get; private set; }

        public string ClientId { get; private set; }

        public short RequiredAcks { get; private set; }

        public int AckTimeoutMs { get; private set; }

        public IDictionary<TopicAndPartition, ByteBufferMessageSet> Data { get; set; }

        public ProducerRequest(
            int correlationId,
            string clientId,
            short requiredAcks,
            int ackTimeoutMs,
            IDictionary<TopicAndPartition, ByteBufferMessageSet> data)
            : base(RequestKeys.ProduceKey, correlationId)
        {
            this.VersionId = CurrentVersion;
            this.CorrelationId = correlationId;
            this.ClientId = clientId;
            this.RequiredAcks = requiredAcks;
            this.AckTimeoutMs = ackTimeoutMs;
            this.Data = data;

            this.dataGroupedByTopic =
                new Lazy<IDictionary<string, IDictionary<TopicAndPartition, ByteBufferMessageSet>>>(
                    () => this.Data.GroupByScala(x => x.Key.Topic));

            this.topicPartitionMessageSizeMap = this.Data.ToDictionary(r => r.Key, v => v.Value.SizeInBytes);
        }

        private readonly Lazy<IDictionary<string, IDictionary<TopicAndPartition, ByteBufferMessageSet>>> dataGroupedByTopic;

        private readonly Dictionary<TopicAndPartition, int> topicPartitionMessageSizeMap;

        public int NumPartitions
        {
            get
            {
                return this.Data.Count();
            }
        }

        public override void WriteTo(MemoryStream buffer)
        {
            buffer.PutShort(this.VersionId);
            buffer.PutInt(this.CorrelationId);
            ApiUtils.WriteShortString(buffer, this.ClientId);
            buffer.PutShort(this.RequiredAcks);
            buffer.PutInt(this.AckTimeoutMs);

            // save the topic structure
            buffer.PutInt(this.dataGroupedByTopic.Value.Count); // the number of topics
            foreach (var kvp in this.dataGroupedByTopic.Value)
            {
                var topic = kvp.Key;
                var topicAndPartitonData = kvp.Value;
                ApiUtils.WriteShortString(buffer, topic); // write the topic
                buffer.PutInt(topicAndPartitonData.Count);
                foreach (var partitionAndData in topicAndPartitonData)
                {
                    var partition = partitionAndData.Key.Partiton;
                    var partitionMessageData = partitionAndData.Value;
                    var bytes = partitionMessageData.Buffer;
                    buffer.PutInt(partition);
                    buffer.PutInt((int)bytes.Length);
                    buffer.Write(bytes.GetBuffer(), 0, (int)bytes.Length);
                    bytes.Position = 0;
                }
            }
        }

        public override int SizeInBytes
        {
            get
            {
                return 2 + /* versionId */ 
                    4 + /* correlationId */ 
                    ApiUtils.ShortStringLength(this.ClientId) + /* client id */ 
                    2 + /* requiredAcks */ 
                    4 + /* ackTimeoutMs */
                    4 + /* number of topics */ 
                    this.dataGroupedByTopic.Value.Aggregate(
                        0,
                       (foldedTopics, currTopic) => foldedTopics
                        + ApiUtils.ShortStringLength(currTopic.Key)
                        + 4 + /* the number of partions */ currTopic.Value.Aggregate(
                             0,
                            (foldedPartitions, currPartition) => foldedPartitions +
                             4 + /* partition id */ 
                             4 + /* byte-length of serialized messages */
                                  currPartition.Value.SizeInBytes));
            }
        }

        public override string ToString()
        {
            return this.Describe(true);
        }

        public override string Describe(bool details)
        {
            var producerRequest = new StringBuilder();
            producerRequest.Append("Name: " + this.GetType().Name);
            producerRequest.Append("; Version: " + this.VersionId);
            producerRequest.Append("; CorrelationId: " + this.CorrelationId);
            producerRequest.Append("; ClientId: " + this.ClientId);
            producerRequest.Append("; RequiredAcks: " + this.RequiredAcks);
            producerRequest.Append("; AckTimeoutMs: " + this.AckTimeoutMs + " ms");
            if (details)
            {
                producerRequest.Append("; TopicAndPartition " + string.Join(", ", this.topicPartitionMessageSizeMap));
            }

            return producerRequest.ToString();
        }

        public void EmptyData()
        {
            this.Data.Clear();
        }
    }
}