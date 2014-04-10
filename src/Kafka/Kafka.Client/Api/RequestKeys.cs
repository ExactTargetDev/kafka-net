namespace Kafka.Client.Api
{
    using System;
    using System.IO;

    public static class RequestKeys
    {
        public const short ProduceKey = 0;

        public const short FetchKey = 1;

        public const short OffsetsKey = 2;

        public const short MetadataKey = 3;

        public const short LeaderAndIsrKey = 4;

        public const short StopReplicaKey = 5;

        public const short UpdateMetadataKey = 6;

        public const short ControllerShutdownKey = 7;

        public const short OffsetCommitKey = 8;

        public const short OffsetFetchKey = 9;

        /* TODO
         * val keyToNameAndDeserializerMap: Map[Short, (String, (ByteBuffer) => RequestOrResponse)]=
    Map(ProduceKey -> ("Produce", ProducerRequest.readFrom),
        FetchKey -> ("Fetch", FetchRequest.readFrom),
        OffsetsKey -> ("Offsets", OffsetRequest.readFrom),
        MetadataKey -> ("Metadata", TopicMetadataRequest.readFrom),
        LeaderAndIsrKey -> ("LeaderAndIsr", LeaderAndIsrRequest.readFrom),
        StopReplicaKey -> ("StopReplica", StopReplicaRequest.readFrom),
        UpdateMetadataKey -> ("UpdateMetadata", UpdateMetadataRequest.readFrom),
        ControlledShutdownKey -> ("ControlledShutdown", ControlledShutdownRequest.readFrom),
        OffsetCommitKey -> ("OffsetCommit", OffsetCommitRequest.readFrom),
        OffsetFetchKey -> ("OffsetFetch", OffsetFetchRequest.readFrom))

         * 

  }*/

        public static string NameForKey(short key)
        {
            throw new NotImplementedException();
        }

        public static Func<MemoryStream, RequestOrResponse> DeserializerForKey(short key)
        {
            throw new NotImplementedException();
        }

    }
}