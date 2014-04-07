namespace Kafka.Client.Common
{
    public static class ErrorMapping
    {
        public const int UnknownCode = -1;
        public const int NoError = 0;
        public const int OffsetOutOfRangeCode = 1;
        public const int InvalidMessageCode = 2;
        public const int UnknownTopicOrPartitionCode = 3;
        public const int InvalidFetchSizeCode = 4;

        //TODO: finish me
    }
}