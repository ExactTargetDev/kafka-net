using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Kafka.Client.Common;

namespace Kafka.Client.Utils
{
    public class Pool<TKey, TValue> : ConcurrentDictionary<TKey, TValue>
    {
        public Func<TKey, TValue> ValueFactory { get; set; }

        public Pool(Func<TKey, TValue> valueFactory = null)
        {
            ValueFactory = valueFactory;
        }

        public TValue GetAndMaybePut(TKey key)
        {
            if (ValueFactory == null)
            {
                throw new KafkaException("Empty value factory in pool");
            }
            return GetOrAdd(key, ValueFactory);
        }
    }
}