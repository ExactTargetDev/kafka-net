namespace Kafka.Client.Common
{
    using System;
    using System.Collections.Generic;

    using System.Linq;

    using Kafka.Client.Messages;

    /// <summary>
    /// A bi-directional mapping between error codes and exceptions x
    /// </summary>
    public static class ErrorMapping
    {
        static ErrorMapping()
        {
            ExceptionToCode = new Dictionary<Type, short>
                                  {
                                      { typeof(OffsetOutOfRangeException), OffsetOutOfRangeCode },
                                      { typeof(InvalidMessageException), InvalidMessageCode },
                                      { typeof(UnknownTopicOrPartitionException), UnknownTopicOrPartitionCode },
                                      { typeof(InvalidMessageSizeException), InvalidFetchSizeCode },
                                      { typeof(NotLeaderForPartitionException), NotLeaderForPartitionCode },
                                      { typeof(LeaderNotAvailableException), LeaderNotAvailableCode },
                                      { typeof(RequestTimedOutException), RequestTimedOutCode },
                                      { typeof(BrokerNotAvailableException), BrokerNotAvailableCode },
                                      { typeof(ReplicaNotAvailableException), ReplicaNotAvailableCode },
                                      { typeof(MessageSizeTooLargeException), MessageSizeTooLargeCode },
                                      { typeof(ControllerMovedException), StaleControllerEpochCode },
                                      { typeof(OffsetMetadataTooLargeException), OffsetMetadataTooLargeCode }
                                  };
            /* invert the mapping */
            CodeToException = ExceptionToCode.ToDictionary(x => x.Value, x => x.Key);
        }

        public const short UnknownCode = -1;

        public const short NoError = 0;

        public const short OffsetOutOfRangeCode = 1;

        public const short InvalidMessageCode = 2;

        public const short UnknownTopicOrPartitionCode = 3;

        public const short InvalidFetchSizeCode = 4;

        public const short LeaderNotAvailableCode = 5;

        public const short NotLeaderForPartitionCode = 6;

        public const short RequestTimedOutCode = 7;

        public const short BrokerNotAvailableCode = 8;

        public const short ReplicaNotAvailableCode = 9;

        public const short MessageSizeTooLargeCode = 10;

        public const short StaleControllerEpochCode = 11;

        public const short OffsetMetadataTooLargeCode = 12;

        public const short StaleLeaderEpochCode = 13;
       
        public static short CodeFor(Type type)
        {
            short code;
            return ExceptionToCode.TryGetValue(type, out code) ? code : UnknownCode;
        }

        public static void MaybeThrowException(short code)
        {
            if (code != 0)
            {
                throw ExceptionFor(code);
            }
        }

        public static Exception ExceptionFor(short code)
        {
            Type exceptionType;
            return (Exception)Activator.CreateInstance(CodeToException.TryGetValue(code, out exceptionType) ? exceptionType : typeof(UnknownException));
        }

        private static readonly Dictionary<Type, short> ExceptionToCode;

        private static readonly Dictionary<short, Type> CodeToException;
    }
}