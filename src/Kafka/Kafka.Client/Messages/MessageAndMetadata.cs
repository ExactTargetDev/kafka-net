namespace Kafka.Client.Messages
{
    using Kafka.Client.Serializers;

    public class MessageAndMetadata<TKey, TValue>
    {
        public string Topic { get; private set; }

        public int Partition { get; private set; }

        private readonly Message rawMessage;

        public long Offset { get; private set; }

        public IDecoder<TKey> KeyDecoder { get; private set; }

        public IDecoder<TValue> ValueDecoder { get; private set; }

        public MessageAndMetadata(string topic, int partition, Message rawMessage, long offset, IDecoder<TKey> keyDecoder, IDecoder<TValue> valueDecoder)
        {
            this.Topic = topic;
            this.Partition = partition;
            this.rawMessage = rawMessage;
            this.Offset = offset;
            this.KeyDecoder = keyDecoder;
            this.ValueDecoder = valueDecoder;
         } 

        public TKey Key 
        {
            get 
            {
                return this.rawMessage.Key != null ? this.KeyDecoder.FromBytes(Utils.Util.ReadBytes(this.rawMessage.Key)) : default(TKey);
            }
        }

        public TValue Message
        {
            get { return this.rawMessage.IsNull() ? default(TValue) : this.ValueDecoder.FromBytes(Utils.Util.ReadBytes(this.rawMessage.Payload)); }
        }
    }
}