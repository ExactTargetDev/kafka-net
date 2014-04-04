namespace Kafka.Client.Consumers
{
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Threading;

    using Kafka.Client.Common.Imported;
    using Kafka.Client.Messages;
    using Kafka.Client.Serializers;
    using Kafka.Client.Utils;

    using log4net;

    /// <summary>
    ///  An iterator that blocks until a value can be read from the supplied queue.
    /// The iterator takes a shutdownCommand object which can be added to the queue to trigger a shutdown
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    internal class ConsumerIterator<TKey, TValue>: IteratorTemplate<MessageAndMetadata<TKey, TValue>>
    {
        private readonly BlockingCollection<FetchedDataChunk> channel;

        private readonly int consumerTimeoutMs;

        private IDecoder<TKey> keyDecoder;
        private IDecoder<TValue> valueDecoder;

        public string ClientId { get; set; }

        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        
        private AtomicReference<IEnumerator<MessageAndOffset>> current = new AtomicReference<IEnumerator<MessageAndOffset>>(null);

        private PartitionTopicInfo currentTopicInfo;

        private long consumedOffset = -1;

        public ConsumerIterator(BlockingCollection<FetchedDataChunk> channel, int consumerTimeoutMs, IDecoder<TKey> keyDecoder, IDecoder<TValue> valueDecoder, string clientId)
        {
            this.channel = channel;
            this.consumerTimeoutMs = consumerTimeoutMs;
            this.keyDecoder = keyDecoder;
            this.valueDecoder = valueDecoder;
            this.ClientId = clientId;
        }

        //TODO: private val consumerTopicStats = ConsumerTopicStatsRegistry.getConsumerTopicStat(clientId)

        protected override MessageAndMetadata<TKey, TValue> MakeNext()
        {
            throw new System.NotImplementedException();
        }

        public void ClearCurrentChunk()
        {
            Logger.Debug("Clearing the current data chunk for this consumer iterator");
            current.Set(null);
        }


    }
}