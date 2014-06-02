using System.Configuration;

using Kafka.Client.Api;
using Kafka.Client.Cfg.Sections;
using Kafka.Client.Common.Imported;
using Kafka.Client.Producers;
using Kafka.Tests.Api;

using Xunit;

namespace Kafka.Tests.Api
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    using Kafka.Client.Api;
    using Kafka.Client.Clusters;
    using Kafka.Client.Common;
    using Kafka.Client.Messages;

    internal static class SerializationTestUtils
    {
        private static readonly string Topic1 = "test1";

        private static readonly string Topic2 = "test2";

        private static readonly ByteBufferMessageSet PartitionDataMessage0 = new ByteBufferMessageSet(new List<Message> { new Message(Encoding.UTF8.GetBytes("first message")) });

        private static readonly ByteBufferMessageSet PartitionDataMessage1 = new ByteBufferMessageSet(new List<Message> { new Message(Encoding.UTF8.GetBytes("second message")) });

        private static readonly ByteBufferMessageSet PartitionDataMessage2 = new ByteBufferMessageSet(new List<Message> { new Message(Encoding.UTF8.GetBytes("third message")) });

        private static readonly ByteBufferMessageSet PartitionDataMessage3 = new ByteBufferMessageSet(new List<Message> { new Message(Encoding.UTF8.GetBytes("fourth message")) });

        private static readonly List<ByteBufferMessageSet> PartitionDataProducerRequestArray = new List<ByteBufferMessageSet> { PartitionDataMessage0, PartitionDataMessage1, PartitionDataMessage2, PartitionDataMessage3 };

        private static IDictionary<TopicAndPartition, ByteBufferMessageSet> TopicDataProducerRequest = new List<string> { Topic1, Topic2 }.SelectMany(
                    topic =>
                    PartitionDataProducerRequestArray.Select(
                        (partitionDataMessage, i) => Tuple.Create(new TopicAndPartition(topic, i), partitionDataMessage)))
                                                   .ToDictionary(x => x.Item1, x => x.Item2);

        private static IDictionary<TopicAndPartition, PartitionFetchInfo> requestInfos = new Dictionary<TopicAndPartition, PartitionFetchInfo>
                                                                                             {
                                                                                                 { new TopicAndPartition(Topic1, 0), new PartitionFetchInfo(1000, 100) },
                                                                                                 { new TopicAndPartition(Topic1, 1), new PartitionFetchInfo(2000, 100) },
                                                                                                 { new TopicAndPartition(Topic1, 2), new PartitionFetchInfo(3000, 100) },
                                                                                                 { new TopicAndPartition(Topic1, 3), new PartitionFetchInfo(4000, 100) },
                                                                                                 { new TopicAndPartition(Topic2, 0), new PartitionFetchInfo(1000, 100) },
                                                                                                 { new TopicAndPartition(Topic2, 1), new PartitionFetchInfo(2000, 100) },
                                                                                                 { new TopicAndPartition(Topic2, 2), new PartitionFetchInfo(3000, 100) },
                                                                                                 { new TopicAndPartition(Topic2, 3), new PartitionFetchInfo(4000, 100) },

                                                                                             };

        private  static readonly  List<Broker> Brokers = new List<Broker>{new Broker(0, "localhost", 1011), new Broker(1, "localhost", 1012), new Broker(2, "localhost", 1013)};

        private static readonly PartitionMetadata PartitionMetaData0 = new PartitionMetadata(0, Brokers[0], Brokers, Brokers);

        private static readonly PartitionMetadata PartitionMetaData1 = new PartitionMetadata(1, Brokers[0], Brokers, new List<Broker> { Brokers.Last() });

        private static readonly PartitionMetadata PartitionMetaData2 = new PartitionMetadata(2, Brokers[0], Brokers, Brokers, 0);

        private static readonly PartitionMetadata PartitionMetaData3 = new PartitionMetadata(3, Brokers[0], Brokers, new List<Broker> { Brokers[Brokers.Count() - 1]});

        private static List<PartitionMetadata> PartitionMetaDataSeq = new List<PartitionMetadata> { PartitionMetaData0, PartitionMetaData1, PartitionMetaData2, PartitionMetaData3};

        private static readonly TopicMetadata TopicmetaData1 = new TopicMetadata(Topic1, PartitionMetaDataSeq);

        private static readonly TopicMetadata TopicmetaData2 = new TopicMetadata(Topic2, PartitionMetaDataSeq);

        public static ProducerRequest CreateTestProducerRequest()
         {
             return new ProducerRequest(1, "client 1", 0, 1000, TopicDataProducerRequest);
         }

        public static ProducerResponse CreateTestProducerResponse()
        {
            return
                new ProducerResponse(
                    new Dictionary<TopicAndPartition, ProducerResponseStatus>
                        {
                            { new TopicAndPartition(Topic1, 0), new ProducerResponseStatus(0, 10001) },
                                                { new TopicAndPartition(Topic2, 0), new ProducerResponseStatus(0, 20001) }
                                            },
                                            1);
        }

        public static FetchRequest CreateTestFetchRequest()
        {
            return new FetchRequest(requestInfo: requestInfos);
        }

        public static OffsetRequest CreateTestOffsetRequest()
        {
            return
                new OffsetRequest(
                    new Dictionary<TopicAndPartition, PartitionOffsetRequestInfo>
                        {
                            {
                                new TopicAndPartition(Topic1, 1), 
                                new PartitionOffsetRequestInfo(
                                1000, 200)
                            }
                        }, 
                    replicaId: 0);
        }

        public static OffsetResponse CreateTestOffsetResponse()
        {
            return new OffsetResponse(
                0,
                new Dictionary<TopicAndPartition, PartitionOffsetsResponse>
                    {
                        {
                            new TopicAndPartition(Topic1, 1),
                            new PartitionOffsetsResponse(
                            ErrorMapping.NoError,
                            new List<long>
                                {
                                    1000L,
                                    2000L,
                                    3000L,
                                    4000L
                                })
                        }
                    });
        }

        public static TopicMetadataRequest CreateTestTopicMetadataRequest()
        {
            return new TopicMetadataRequest(1, 1, "client 1", new List<string> { Topic1, Topic2 });
        }

        public static TopicMetadataResponse CreateTestTopicMetadataResponse()
        {
            return new TopicMetadataResponse(new List<TopicMetadata> { TopicmetaData1, TopicmetaData2 }, 1);
        }
    }
}

public class RequestResponseSerializationTest
{
    private readonly ProducerRequest producerRequest;

    private readonly ProducerResponse producerResponse;

    private readonly FetchRequest fetchRequest;

    private readonly OffsetRequest offsetRequest;

    private readonly OffsetResponse offsetResponse;

    private readonly TopicMetadataRequest topicMetadataRequest;

    private readonly TopicMetadataResponse topicMetadataResponse;

    public RequestResponseSerializationTest()
    {
        this.producerRequest = SerializationTestUtils.CreateTestProducerRequest();
        this.producerResponse = SerializationTestUtils.CreateTestProducerResponse();
        this.fetchRequest = SerializationTestUtils.CreateTestFetchRequest();
        this.offsetRequest = SerializationTestUtils.CreateTestOffsetRequest();
        this.offsetResponse = SerializationTestUtils.CreateTestOffsetResponse();
        this.topicMetadataRequest = SerializationTestUtils.CreateTestTopicMetadataRequest();
        this.topicMetadataResponse = SerializationTestUtils.CreateTestTopicMetadataResponse();
    }

    [Fact]
    public void TestSerializationAndDeserialization()
    {
        var buffer = ByteBuffer.Allocate(this.producerRequest.SizeInBytes);
        this.producerRequest.WriteTo(buffer);
        buffer.Rewind();
        var deserializedProducerRequest = ProducerRequest.ReadFrom(buffer);
        Assert.Equal(this.producerRequest, deserializedProducerRequest);

        buffer = ByteBuffer.Allocate(this.producerResponse.SizeInBytes);
        this.producerResponse.WriteTo(buffer);
        buffer.Rewind();
        var deserializedProducerResponse = ProducerResponse.ReadFrom(buffer);
        Assert.Equal(this.producerResponse, deserializedProducerResponse);

        buffer = ByteBuffer.Allocate(this.fetchRequest.SizeInBytes);
        this.fetchRequest.WriteTo(buffer);
        buffer.Rewind();
        var deserializedFetchRequest = FetchRequest.ReadFrom(buffer);
        Assert.Equal(this.fetchRequest, deserializedFetchRequest);

        buffer = ByteBuffer.Allocate(this.offsetRequest.SizeInBytes);
        this.offsetRequest.WriteTo(buffer);
        buffer.Rewind();
        var deserializedOffsetRequest = OffsetRequest.ReadFrom(buffer);
        Assert.Equal(this.offsetRequest, deserializedOffsetRequest);

        buffer = ByteBuffer.Allocate(this.offsetResponse.SizeInBytes);
        this.offsetResponse.WriteTo(buffer);
        buffer.Rewind();
        var deserializedOffsetResponse = OffsetResponse.ReadFrom(buffer);
        Assert.Equal(this.offsetResponse, deserializedOffsetResponse);

        buffer = ByteBuffer.Allocate(this.topicMetadataRequest.SizeInBytes);
        this.topicMetadataRequest.WriteTo(buffer);
        buffer.Rewind();
        var deserializedTopicMetadataRequest = TopicMetadataRequest.ReadFrom(buffer);
        Assert.Equal(this.topicMetadataRequest, deserializedTopicMetadataRequest);

        buffer = ByteBuffer.Allocate(this.topicMetadataResponse.SizeInBytes);
        this.topicMetadataResponse.WriteTo(buffer);
        buffer.Rewind();
        var deserializedTopicMetadataResponse = TopicMetadataResponse.ReadFrom(buffer);
        Assert.Equal(this.topicMetadataResponse, deserializedTopicMetadataResponse);
    }
}