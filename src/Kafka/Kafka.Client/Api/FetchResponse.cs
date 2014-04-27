namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    using Kafka.Client.Common;

    using Kafka.Client.Extensions;

    using System.Linq;

    using Kafka.Client.Messages;
    using Kafka.Client.Network;

    internal class FetchResponse
    {

        public const int HeaderSize = 4 + /* correlationId */ 4 /* topic count */;

        public int CorrelationId { get; private set; }

        public IDictionary<TopicAndPartition, FetchResponsePartitionData> Data { get; private set; }

        public FetchResponse(int correlationId, IDictionary<TopicAndPartition, FetchResponsePartitionData> data)
        {
            this.CorrelationId = correlationId;
            this.Data = data;

            this.dataGroupedByTopic =
                new Lazy<IDictionary<string, IDictionary<TopicAndPartition, FetchResponsePartitionData>>>(
                    () => this.Data.GroupByScala(x => x.Key.Topic));

        }

        private Lazy<IDictionary<string, IDictionary<TopicAndPartition, FetchResponsePartitionData>>> dataGroupedByTopic; 

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
            return Equals((FetchResponse)obj);
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
                return FetchResponse.HeaderSize +
                    this.dataGroupedByTopic.Value.Aggregate(0, (folded, curr) =>
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
                throw new ArgumentException(string.Format("No partition {0} in fetch response {1}", topicAndPartition, this.ToString()));
            }
        }

        public ByteBufferMessageSet MessageSet(string topic, int partition)
        {
            return (ByteBufferMessageSet) this.PartitionDataFor(topic, partition).Messages;
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

        public static FetchResponse ReadFrom(MemoryStream buffer)
        {
            var correlationId = buffer.GetInt();
            var topicCount = buffer.GetInt();
            var pairs = Enumerable.Range(1, topicCount).SelectMany(_ =>
                {
                    var topicData = TopicData.ReadFrom(buffer);
                    return topicData.PartitionData.Select(partIdAndData =>
                        {
                            return Tuple.Create(
                                new TopicAndPartition(topicData.Topic, partIdAndData.Key), partIdAndData.Value);
                        });

                });
            return new FetchResponse(correlationId, pairs.ToDictionary(k => k.Item1, v => v.Item2));
        }

    }

    internal class TopicData
    {
        public static TopicData ReadFrom(MemoryStream buffer)
        {
            var topic = ApiUtils.ReadShortString(buffer);
            var partitionCount = buffer.GetInt();
            var topicPartitionDataPairs = Enumerable.Range(1, partitionCount).Select(
                _ =>
                    {
                        var partitonId = buffer.GetInt();
                        var partitionData = FetchResponsePartitionData.ReadFrom(buffer);
                        return Tuple.Create(partitonId, partitionData);
                    });
            return new TopicData(topic, topicPartitionDataPairs.ToDictionary(k => k.Item1, v => v.Item2));
        }

        public static int GetHeaderSize(string topic)
        {
            return ApiUtils.ShortStringLength(topic) + 4 /* partition count */
            ;
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
                return TopicData.GetHeaderSize(Topic)
                       + PartitionData.Values.Aggregate(0, (first, second) => first + second.SizeInBytes + 4);
            }
        }

        public int HeaderSize
        {
            get
            {
                return TopicData.GetHeaderSize(Topic);
            }
        }
    }

    internal class FetchResponseSend : Send
    {
        public FetchResponse FetchResponse { get; private set; }

        public FetchResponseSend(FetchResponse fetchResponse)
        {
            this.FetchResponse = fetchResponse;

            this.size = FetchResponse.SizeInBytes;
            this.sent = 0;
            this.sendSize = 4 /* for size */ + this.size;

            //TODO: assign buffer
        }

        private int size;

        private int sent;

        private readonly int sendSize;

        public bool Complete
        {
            get
            {
                return sent >= sendSize;
            }
        }

        private MemoryStream buffer;

        //TODO: val sends = new MultiSend

        public override int WriteTo(Stream channel)
        {
            this.ExpectIncomplete();
            var written = 0;
            throw new NotImplementedException();
            /* TODO:
    if(buffer.hasRemaining)
      written += channel.write(buffer)
    if(!buffer.hasRemaining && !sends.complete) {
      written += sends.writeTo(channel)
    }
    sent += written
    written*/
        }


    }
 

}