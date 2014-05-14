namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Extensions;
    using Kafka.Client.Messages;

    internal class ProducerRequest : RequestOrResponse
    {
        public const short CurrentVersion = 0;

        public static ProducerRequest ReadFrom(ByteBuffer buffer)
        {
            var versionId = buffer.GetShort();
            var correlationId = buffer.GetInt();
            var clientId = ApiUtils.ReadShortString(buffer);
            var requiredAcks = buffer.GetShort();
            var ackTimeoutMs = buffer.GetInt();

            // built the topic structure
            var topicCount = buffer.GetInt();
            var partitionDataPairs = Enumerable.Range(1, topicCount).SelectMany(_ =>
            {
                // process topic
                var topic = ApiUtils.ReadShortString(buffer);
                var partitionCount = buffer.GetInt();
                return Enumerable.Range(1, partitionCount).Select(__ =>
                {
                    var partition = buffer.GetInt();
                    var messagesSetSize = buffer.GetInt();
                    var messageSetBuffer = new byte[messagesSetSize];
                    buffer.Get(messageSetBuffer, 0, messagesSetSize);
                    return Tuple.Create(
                        new TopicAndPartition(topic, partition),
                        new ByteBufferMessageSet(ByteBuffer.Wrap(messageSetBuffer)));
                });
            });
            return new ProducerRequest(versionId, correlationId, clientId, requiredAcks, ackTimeoutMs, partitionDataPairs.ToDictionary(k => k.Item1, v => v.Item2));
        }

        public short VersionId { get; private set; }

        public string ClientId { get; private set; }

        public short RequiredAcks { get; private set; }

        public int AckTimeoutMs { get; private set; }

        public IDictionary<TopicAndPartition, ByteBufferMessageSet> Data { get; set; }

        private readonly Lazy<IDictionary<string, IDictionary<TopicAndPartition, ByteBufferMessageSet>>> dataGroupedByTopic;

        private readonly Dictionary<TopicAndPartition, int> topicPartitionMessageSizeMap;

        public ProducerRequest(
            int correlationId,
            string clientId,
            short requiredAcks,
            int ackTimeoutMs,
            IDictionary<TopicAndPartition, ByteBufferMessageSet> data)
            : this(CurrentVersion, correlationId, clientId, requiredAcks, ackTimeoutMs, data)
        {
        }

        public ProducerRequest(
            short versionId,
            int correlationId,
            string clientId,
            short requiredAcks,
            int ackTimeoutMs,
            IDictionary<TopicAndPartition, ByteBufferMessageSet> data)
            : base(RequestKeys.ProduceKey, correlationId)
        {
            this.VersionId = versionId;
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

        public int NumPartitions
        {
            get
            {
                return this.Data.Count();
            }
        }

        public override void WriteTo(ByteBuffer buffer)
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
                    buffer.Put(bytes);
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

        protected bool Equals(ProducerRequest other)
        {
            return this.VersionId == other.VersionId 
                && string.Equals(this.ClientId, other.ClientId)
                && this.CorrelationId == other.CorrelationId 
                && this.RequiredAcks == other.RequiredAcks 
                && this.AckTimeoutMs == other.AckTimeoutMs 
                && this.Data.DictionaryEqual(other.Data);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }
            if (ReferenceEquals(this, obj))
            {
                return true;
            }
            if (obj.GetType() != this.GetType())
            {
                return false;
            }
            return Equals((ProducerRequest)obj);
        }

        public override int GetHashCode()
        {
            throw new NotSupportedException();
        }
    }
}