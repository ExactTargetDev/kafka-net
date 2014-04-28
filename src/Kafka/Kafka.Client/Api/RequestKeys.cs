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
    }
}