namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;

    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Extensions;

    internal class OffsetResponse : RequestOrResponse
    {
        public static OffsetResponse ReadFrom(ByteBuffer buffer)
        {
            var correlationId = buffer.GetInt();
            var numTopics = buffer.GetInt();
            var pairs = Enumerable.Range(1, numTopics).SelectMany(_ =>
                {
                    var topic = ApiUtils.ReadShortString(buffer);
                    var numPartitions = buffer.GetInt();
                    return Enumerable.Range(1, numPartitions).Select(__ =>
                        {
                            var partiton = buffer.GetInt();
                            var error = buffer.GetShort();
                            var numOffsets = buffer.GetInt();
                            var offsets = Enumerable.Range(1, numOffsets).Select(o => buffer.GetLong()).ToList();
                            return new
                                KeyValuePair<TopicAndPartition, PartitionOffsetsResponse>(
                                    new TopicAndPartition(topic, partiton), new PartitionOffsetsResponse(error, offsets));
                        });
                });
            return new OffsetResponse(correlationId, pairs.ToDictionary(kvp => kvp.Key, kvp => kvp.Value));
        }

        public IDictionary<TopicAndPartition, PartitionOffsetsResponse> PartitionErrorAndOffsets { get; private set; }

        private Lazy<IDictionary<string, IDictionary<TopicAndPartition, PartitionOffsetsResponse>>> offsetsGroupedByTopic; 

        internal OffsetResponse(
            int correlationId, IDictionary<TopicAndPartition, PartitionOffsetsResponse> partitionErrorAndOffsets)
            : base(null, correlationId)
        {
            this.PartitionErrorAndOffsets = partitionErrorAndOffsets;
            this.offsetsGroupedByTopic = new Lazy<IDictionary<string, IDictionary<TopicAndPartition, PartitionOffsetsResponse>>>(
                () => this.PartitionErrorAndOffsets.GroupByScala(p => p.Key.Topic));
        }

        public bool HasError
        {
            get
            {
                return this.PartitionErrorAndOffsets.Values.Any(v => v.Error != ErrorMapping.NoError);
            }
        }

        public override int SizeInBytes
        {
            [SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1407:ArithmeticExpressionsMustDeclarePrecedence", Justification = "Reviewed. Suppression is OK here.")]
            get 
            {
               return 4 + /* correlation id */
               4 + /* topic count */
               this.offsetsGroupedByTopic.Value.Aggregate(
               0, 
               (foldedTopic, currTopic) =>
                   {
                       var topic = currTopic.Key;
                       var errorAndOffsetsMap = currTopic.Value;
                       return foldedTopic +
                           ApiUtils.ShortStringLength(topic) +
                           4 + /* partition count */
                           errorAndOffsetsMap.Aggregate(
                           0, 
                           (foldedPartitions, currPartition) =>
                               {
                                   return foldedPartitions +
                                    4 + /* partition id */
                                    2 + /* partition error */
                                    4 + /* offset array length */
                                    currPartition.Value.Offsets.Count * 8; /* offset */
                               });
                   });
            }
        }

        public override void WriteTo(ByteBuffer buffer)
        {
            buffer.PutInt(CorrelationId);
            buffer.PutInt(offsetsGroupedByTopic.Value.Count); // topic count
            foreach (var kvp in offsetsGroupedByTopic.Value)
            {
                var topic = kvp.Key;
                var errorAndOffsetsMap = kvp.Value;
                ApiUtils.WriteShortString(buffer, topic);
                buffer.PutInt(errorAndOffsetsMap.Count);
                foreach (var topicPartitionAndErrorOffset in errorAndOffsetsMap)
                {
                    buffer.PutInt(topicPartitionAndErrorOffset.Key.Partiton);
                    buffer.PutShort(topicPartitionAndErrorOffset.Value.Error);
                    buffer.PutInt(topicPartitionAndErrorOffset.Value.Offsets.Count);
                    foreach (var offset in topicPartitionAndErrorOffset.Value.Offsets)
                    {
                        buffer.PutLong(offset);
                    }
                }
            }
        }

        public override string Describe(bool details)
        {
            return this.ToString();
        }

        protected bool Equals(OffsetResponse other)
        {
            return this.CorrelationId == other.CorrelationId
                   && this.PartitionErrorAndOffsets.DictionaryEqual(other.PartitionErrorAndOffsets);
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
            return Equals((OffsetResponse)obj);
        }

        public override int GetHashCode()
        {
            throw new NotSupportedException();
        }
    }

    internal class PartitionOffsetsResponse
    {
        public short Error { get; private set; }

        public List<long> Offsets { get; private set; }

        public PartitionOffsetsResponse(short error, List<long> offsets)
        {
            this.Error = error;
            this.Offsets = offsets;
        }

        protected bool Equals(PartitionOffsetsResponse other)
        {
            return this.Error == other.Error && this.Offsets.SequenceEqual(other.Offsets);
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

            return this.Equals((PartitionOffsetsResponse)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (this.Error.GetHashCode() * 397) ^ (this.Offsets != null ? this.Offsets.GetHashCode() : 0);
            }
        }

        public override string ToString()
        {
            return string.Format("Error: {0}, Offsets: {1}", ErrorMapping.ExceptionFor(this.Error), string.Join(",", this.Offsets));
        }
    }
}