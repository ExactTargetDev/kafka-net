namespace Kafka.Client.Consumers
{
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;

    using Kafka.Client.Common.Imported;
    using Kafka.Client.Messages;
    using Kafka.Client.Serializers;

    public abstract class KafkaStream
    {
        public abstract void Clear();
    }

    public class KafkaStream<K, V> : KafkaStream , IEnumerable<MessageAndMetadata<K, V>>
    {
        private readonly BlockingCollection<FetchedDataChunk> queue;

        private readonly IDecoder<K> keyDecoder;

        private readonly IDecoder<V> valueDecoder;

        public string ClientId { get; set; }

        public KafkaStream(BlockingCollection<FetchedDataChunk> queue, int consumerTimeoutMs, IDecoder<K> keyDecoder, IDecoder<V> valueDecoder, string clientId)
        {
            this.queue = queue;
            this.keyDecoder = keyDecoder;
            this.valueDecoder = valueDecoder;
            this.ClientId = clientId;
            this.iter = new ConsumerIterator<K, V>(queue, consumerTimeoutMs, keyDecoder, valueDecoder, clientId);
        }

        private ConsumerIterator<K, V> iter;

        /// <summary>
        /// This method clears the queue being iterated during the consumer rebalancing. This is mainly
        ///  to reduce the number of duplicates received by the consumer
        /// </summary>
        public override void Clear()
        {
            iter.ClearCurrentChunk();
        }

        public IEnumerator<MessageAndMetadata<K, V>> GetEnumerator()
        {
            if (iter.HasNext())
            {
                yield return iter.Next();
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }
}