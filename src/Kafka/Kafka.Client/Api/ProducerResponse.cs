namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;

    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Extensions;

    public class ProducerResponse : RequestOrResponse
    {
        public static ProducerResponse ReadFrom(ByteBuffer buffer)
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

        public Dictionary<TopicAndPartition, ProducerResponseStatus> Status { get; private set; }

        private readonly Lazy<IDictionary<string, IDictionary<TopicAndPartition, ProducerResponseStatus>>> statusGroupedByTopic;

        public ProducerResponse(Dictionary<TopicAndPartition, ProducerResponseStatus> status, int correlationId)
            : base(null, correlationId)
        {
            this.Status = status;

            this.statusGroupedByTopic = new Lazy<IDictionary<string, IDictionary<TopicAndPartition, ProducerResponseStatus>>>(() =>
                this.Status.GroupByScala(x => x.Key.Topic));
        }

        public bool HasError()
        {
            return this.Status.Values.Any(v => v.Error != ErrorMapping.NoError);
        }

        public override int SizeInBytes
        {
            [SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1407:ArithmeticExpressionsMustDeclarePrecedence", Justification = "Reviewed. Suppression is OK here.")]
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

        public override void WriteTo(ByteBuffer buffer)
        {
            var groupedStatus = this.statusGroupedByTopic.Value;
            buffer.PutInt(this.CorrelationId);
            buffer.PutInt(groupedStatus.Count); // topic count

            foreach (var topicStatus in groupedStatus)
            {
                var topic = topicStatus.Key;
                var errorsAndOffsets = topicStatus.Value;
                ApiUtils.WriteShortString(buffer, topic);
                buffer.PutInt(errorsAndOffsets.Count); // partition count
                foreach (var kvp in errorsAndOffsets)
                {
                    buffer.PutInt(kvp.Key.Partiton);
                    buffer.PutShort(kvp.Value.Error);
                    buffer.PutLong(kvp.Value.Offset);
                }
            }
        }

        public override string Describe(bool details)
        {
            return this.ToString();
        }

        protected bool Equals(ProducerResponse other)
        {
            return this.CorrelationId == other.CorrelationId && this.Status.DictionaryEqual(other.Status);
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

            return Equals((ProducerResponse)obj);
        }
    }

    public class ProducerResponseStatus
    {
        public short Error { get; private set; }

        public long Offset { get; private set; }

        public ProducerResponseStatus(short error, long offset)
        {
            this.Error = error;
            this.Offset = offset;
        }

        protected bool Equals(ProducerResponseStatus other)
        {
            return this.Error == other.Error && this.Offset == other.Offset;
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

            return Equals((ProducerResponseStatus)obj);
        }

        public override int GetHashCode()
        {
            throw new NotSupportedException();
        }
    }
}