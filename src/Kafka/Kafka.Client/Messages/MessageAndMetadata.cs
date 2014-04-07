using Kafka.Client.Serializers;

namespace Kafka.Client.Messages
{
    public class MessageAndMetadata<K, V>
    {
        public string Topic { get; private set; }
        public int Partition { get; private set; }
        private readonly Message rawMessage;
        public long Offset { get; private set; }
        public IDecoder<K> KeyDecoder { get; private set; }
        public IDecoder<V> ValueDecoder { get; private set; }


        public MessageAndMetadata(string topic, int partition, Message rawMessage, long offset, IDecoder<K> keyDecoder, IDecoder<V> valueDecoder )
        {
          Topic = topic;
          Partition = partition;
            this.rawMessage = rawMessage;
                Offset = offset;
                KeyDecoder = keyDecoder;
                ValueDecoder = ValueDecoder;
         } 

        public K Key { get 
        {
            return rawMessage.Key != null ? KeyDecoder.FromBytes(Utils.Util.ReadBytes(rawMessage.Key)) : default(K);
        }}

        public V Message
        {
            get { return rawMessage.IsNull() ? default(V) : ValueDecoder.FromBytes(Utils.Util.ReadBytes(rawMessage.Payload)); }
        }
    }
}