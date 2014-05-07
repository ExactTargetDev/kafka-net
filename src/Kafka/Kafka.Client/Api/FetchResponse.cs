namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Extensions;
    using Kafka.Client.Messages;

    internal class FetchResponsePartitionData
    {
        public static FetchResponsePartitionData ReadFrom(ByteBuffer buffer)
        {
            var error = buffer.GetShort();
            var hw = buffer.GetLong();
            var messageSetSize = buffer.GetInt();
            var messageSetBuffer = buffer.Slice();
            messageSetBuffer.Limit(messageSetSize);
            buffer.Position = buffer.Position + messageSetSize;

            return new FetchResponsePartitionData(error, hw, new ByteBufferMessageSet(messageSetBuffer));
        }

        public const int HeaderSize = 2 + /* error code */ 8 + /* high watermark */ 4 /* messageSetSize */;

        public FetchResponsePartitionData(short error = ErrorMapping.NoError, long hw = -1, MessageSet messages = null)
        {
            this.Error = error;
            this.Hw = hw;
            this.Messages = messages;
        }

        public short Error { get; private set; }

        public long Hw { get; private set; }

        public MessageSet Messages { get; private set; }

        public int SizeInBytes
        {
            get
            {
                return HeaderSize + this.Messages.SizeInBytes;
            }
        }
    }

    internal class TopicData
    {
        public static TopicData ReadFrom(ByteBuffer buffer)
        {
            var topic = ApiUtils.ReadShortString(buffer);
            var partitionCount = buffer.GetInt();
            var topicPartitionDataPairs = Enumerable.Range(1, partitionCount).Select(
                _ =>
                {
                    var partitonId = buffer.GetInt();
                    var partitionData = FetchResponsePartitionData.ReadFrom(buffer);
                    return Tuple.Create(partitonId, partitionData);
                }).ToList();
            return new TopicData(topic, topicPartitionDataPairs.ToDictionary(k => k.Item1, v => v.Item2));
        }

        public static int GetHeaderSize(string topic)
        {
            return ApiUtils.ShortStringLength(topic) + 4 /* partition count */;
        }

        public string Topic { get; private set; }

        public IDictionary<int, FetchResponsePartitionData> PartitionData { get; private set; }

        public TopicData(string topic, IDictionary<int, FetchResponsePartitionData> partitionData)
        {
            this.Topic = topic;
            this.PartitionData = partitionData;
        }

        public int SizeInBytes
        {
            get
            {
                return GetHeaderSize(this.Topic)
                       + this.PartitionData.Values.Aggregate(0, (first, second) => first + second.SizeInBytes + 4);
            }
        }

        public int HeaderSize
        {
            get
            {
                return GetHeaderSize(this.Topic);
            }
        }
    }

    internal class FetchResponse
    {
        public const int HeaderSize = 4 + /* correlationId */ 4 /* topic count */;

        public static FetchResponse ReadFrom(ByteBuffer buffer)
        {
            var correlationId = buffer.GetInt();
            var topicCount = buffer.GetInt();
            var pairs = Enumerable.Range(1, topicCount).SelectMany(_ =>
            {
                var topicData = TopicData.ReadFrom(buffer);
                return topicData.PartitionData.Select(partIdAndData => Tuple.Create(
                    new TopicAndPartition(topicData.Topic, partIdAndData.Key), partIdAndData.Value));
            });

            return new FetchResponse(correlationId, pairs.ToDictionary(k => k.Item1, v => v.Item2));
        }

        public int CorrelationId { get; private set; }

        public IDictionary<TopicAndPartition, FetchResponsePartitionData> Data { get; private set; }

        /// <summary>
        /// Partitions the data into a map of maps (one for each topic).
        /// </summary>
        private Lazy<IDictionary<string, IDictionary<TopicAndPartition, FetchResponsePartitionData>>> dataGroupedByTopic; 

        public FetchResponse(int correlationId, IDictionary<TopicAndPartition, FetchResponsePartitionData> data)
        {
            this.CorrelationId = correlationId;
            this.Data = data;

            this.dataGroupedByTopic =
                new Lazy<IDictionary<string, IDictionary<TopicAndPartition, FetchResponsePartitionData>>>(
                    () => this.Data.GroupByScala(x => x.Key.Topic));
        }

        public override string ToString()
        {
            return string.Format("CorrelationId: {0}, Data: {1}", this.CorrelationId, this.Data);
        }

        protected bool Equals(FetchResponse other)
        {
            return this.CorrelationId == other.CorrelationId && Equals(this.Data, other.Data);
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

            return this.Equals((FetchResponse)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (this.CorrelationId * 397) ^ (this.Data != null ? this.Data.GetHashCode() : 0);
            }
        }

        public int SizeInBytes
        {
            get
            {
                return HeaderSize +
                    this.dataGroupedByTopic.Value.Aggregate(
                    0,
                    (folded, curr) =>
                        {
                            var topicData = new TopicData(
                                curr.Key, curr.Value.ToDictionary(k => k.Key.Partiton, v => v.Value));
                            return folded + topicData.SizeInBytes;
                        });
            }
        }

        private FetchResponsePartitionData PartitionDataFor(string topic, int partition)
        {
            var topicAndPartition = new TopicAndPartition(topic, partition);
            FetchResponsePartitionData partitionData;
            if (this.Data.TryGetValue(topicAndPartition, out partitionData))
            {
                return partitionData;
            }
            else
            {
                throw new ArgumentException(string.Format("No partition {0} in fetch response {1}", topicAndPartition, this));
            }
        }

        public ByteBufferMessageSet MessageSet(string topic, int partition)
        {
            return (ByteBufferMessageSet)this.PartitionDataFor(topic, partition).Messages;
        }

        public long HighWatermark(string topic, int partition)
        {
            return this.PartitionDataFor(topic, partition).Hw;
        }

        public bool HasError
        {
            get
            {
                return this.Data.Values.Any(x => x.Error != ErrorMapping.NoError);
            }
        }

        public int ErrorCode(string topic, int partition)
        {
            return this.PartitionDataFor(topic, partition).Error;
        }
    }
}