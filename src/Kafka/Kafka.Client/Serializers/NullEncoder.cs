namespace Kafka.Client.Serializers
{
    using Kafka.Client.Producers;

    public class NullEncoder<T> : IEncoder<T>
    {

        public NullEncoder(ProducerConfig config = null)
        {
        }

        public byte[] ToBytes(T t)
        {
            return null;
        }
    }
}