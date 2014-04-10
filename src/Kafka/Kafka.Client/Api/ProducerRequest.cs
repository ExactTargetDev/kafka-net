namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;

    using Kafka.Client.Common;
    using Kafka.Client.Messages;

    using System.Linq;

    using Kafka.Client.Extensions;

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

            this.dataGroupedByTopic = new Lazy<Dictionary<string, Dictionary<TopicAndPartition, ByteBufferMessageSet>>>(
                () =>
                {

                    var result = new Dictionary<string, Dictionary<TopicAndPartition, ByteBufferMessageSet>>();
                    foreach (var kvp in this.Data)
                    {
                        var topic = kvp.Key.Topic;
                        if (result.ContainsKey(topic) == false)
                        {
                            result[topic] = new Dictionary<TopicAndPartition, ByteBufferMessageSet>();
                        }
                        result[topic][kvp.Key] = kvp.Value;
                    }
                    return result;
                });

            this.topicPartitionMessageSizeMap = this.Data.ToDictionary(r => r.Key, v => v.Value.SizeInBytes);

            this.numPartitions = data.Count();
        }

        private Lazy<Dictionary<string, Dictionary<TopicAndPartition, ByteBufferMessageSet>>> dataGroupedByTopic;

        private Dictionary<TopicAndPartition, int> topicPartitionMessageSizeMap;

        private int numPartitions;

        public override void WriteTo(MemoryStream buffer)
        {
            buffer.PutShort(this.VersionId);
            buffer.PutInt(this.CorrelationId);
            ApiUtils.WriteShortString(buffer, this.ClientId);
            buffer.PutShort(this.RequiredAcks);
            buffer.PutInt(this.AckTimeoutMs);

            //save the topic structure
            buffer.PutInt(dataGroupedByTopic.Value.Count); // the number of topics
            foreach (var kvp in dataGroupedByTopic.Value)
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
                return 2 + /* versionId */ 4 + /* correlationId */ ApiUtils.ShortStringLength(ClientId) + /* client id */ 2
                + /* requiredAcks */ 4 + /* ackTimeoutMs */ 4
                + /* number of topics */ dataGroupedByTopic.Value.Aggregate(
                    0,
                    (foldedTopics, currTopic) => foldedTopics
                        + ApiUtils.ShortStringLength(currTopic.Key)
                        + 4
                         + /* the number of partions */ currTopic.Value.Aggregate(0, (foldedPartitions, currPartition) => foldedPartitions +
                             4 + /* partition id */ 
                             4 + /* byte-length of serialized messages */
                                  currPartition.Value.SizeInBytes));

            }
        }

        public override string ToString()
        {
            return this.Describe(true);
        }

        //TODO: override  def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.RequestChannelRequest): Unit = {

        public override string Describe(bool details)
        {
            var producerRequest = new StringBuilder();
            producerRequest.Append("Name: " + this.GetType().Name);
            producerRequest.Append("; Version: " + VersionId);
            producerRequest.Append("; CorrelationId: " + CorrelationId);
            producerRequest.Append("; ClientId: " + ClientId);
            producerRequest.Append("; RequiredAcks: " + RequiredAcks);
            producerRequest.Append("; AckTimeoutMs: " + AckTimeoutMs + " ms");
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