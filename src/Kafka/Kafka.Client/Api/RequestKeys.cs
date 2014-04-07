namespace Kafka.Client.Api
{
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

  def nameForKey(key: Short): String = {
    keyToNameAndDeserializerMap.get(key) match {
      case Some(nameAndSerializer) => nameAndSerializer._1
      case None => throw new KafkaException("Wrong request type %d".format(key))
    }
  }

  def deserializerForKey(key: Short): (ByteBuffer) => RequestOrResponse = {
    keyToNameAndDeserializerMap.get(key) match {
      case Some(nameAndSerializer) => nameAndSerializer._2
      case None => throw new KafkaException("Wrong request type %d".format(key))
    }
  }*/
    }
}