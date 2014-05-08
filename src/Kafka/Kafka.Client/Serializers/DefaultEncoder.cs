namespace Kafka.Client.Serializers
{
    using Kafka.Client.Cfg;
    using Kafka.Client.Producers;

    /// <summary>
    /// The default implementation is a no-op, it just returns the same array it takes in
    /// </summary>
    public class DefaultEncoder : IEncoder<byte[]>
    {
        public DefaultEncoder(ProducerConfig config)
        {
        }

        public byte[] ToBytes(byte[] t)
        {
            return t;
        }
    }
}