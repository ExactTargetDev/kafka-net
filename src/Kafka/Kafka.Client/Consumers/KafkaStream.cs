namespace Kafka.Client.Consumers
{
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;

    using Kafka.Client.Messages;
    using Kafka.Client.Serializers;

    public class KafkaStream<TKey, TValue> : IEnumerable<MessageAndMetadata<TKey, TValue>>
    {
        private readonly BlockingCollection<FetchedDataChunk> queue;

        private readonly IDecoder<TKey> keyDecoder;

        private readonly IDecoder<TValue> valueDecoder;

        public string ClientId { get; set; }

        public KafkaStream(BlockingCollection<FetchedDataChunk> queue, int consumerTimeoutMs, IDecoder<TKey> keyDecoder, IDecoder<TValue> valueDecoder, string clientId)
        {
            this.queue = queue;
            this.keyDecoder = keyDecoder;
            this.valueDecoder = valueDecoder;
            this.ClientId = clientId;
            this.iter = new ConsumerIterator<TKey, TValue>(queue, consumerTimeoutMs, keyDecoder, valueDecoder, clientId);
        }

        private readonly ConsumerIterator<TKey, TValue> iter;

        /// <summary>
        /// This method clears the queue being iterated during the consumer rebalancing. This is mainly
        ///  to reduce the number of duplicates received by the consumer
        /// </summary>
        public void Clear()
        {
            this.iter.ClearCurrentChunk();
        }

        public IEnumerator<MessageAndMetadata<TKey, TValue>> GetEnumerator()
        {
            if (this.iter.HasNext())
            {
                yield return this.iter.Next();
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }
}