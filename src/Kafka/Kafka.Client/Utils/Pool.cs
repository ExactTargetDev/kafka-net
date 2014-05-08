namespace Kafka.Client.Utils
{
    using System;
    using System.Collections.Concurrent;

    using Kafka.Client.Common;

    public class Pool<TKey, TValue> : ConcurrentDictionary<TKey, TValue>
    {
        public Func<TKey, TValue> ValueFactory { get; set; }

        public Pool(Func<TKey, TValue> valueFactory = null)
        {
            this.ValueFactory = valueFactory;
        }

        public TValue GetAndMaybePut(TKey key)
        {
            if (this.ValueFactory == null)
            {
                throw new KafkaException("Empty value factory in pool");
            }
            return this.GetOrAdd(key, this.ValueFactory);
        }
    }
}