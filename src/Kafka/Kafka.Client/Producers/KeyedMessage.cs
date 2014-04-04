namespace Kafka.Client.Producers
{
    using System;

    /// <summary>
    /// A topic, key, and value.
    /// If a partition key is provided it will override the key for the purpose of partitioning but will not be stored.
    /// </summary>
    public class KeyedMessage<TKey, TValue>
    {
        public string Topic { get; set; }

        public TKey Key { get; set; }

        public object PartKey { get; set; }

        public TValue Message { get; set; }

        public KeyedMessage(string topic, TKey key, object partKey, TValue message)
        {
            this.Topic = topic;
            this.Key = key;
            this.PartKey = partKey;
            this.Message = message;
            if (topic == null)
            {
                throw new ArgumentException("Topic cannot be null", "topic");
            }
        }

        public KeyedMessage(string topic, TValue message)
            : this(topic, default(TKey), null, message)
        {
            
        }

        public KeyedMessage(string topic, TKey key, TValue message)
            : this(topic, key, key, message)
        {

        }

        public object PartitionKey
        {
            get
            {
                if (PartKey != null)
                {
                    return PartKey;
                }
                if (this.HasKey)
                {
                    return this.Key;
                }
                return null;
            }
        }



        public bool HasKey 
        { get
            {
                return this.Key != null;
            }
        }
    }
}