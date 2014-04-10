namespace Kafka.Client.Serializers
{
    using Kafka.Client.Utils;

    public class DefaultDecoder : IDecoder<byte[]>
    {
        public DefaultDecoder(VerifiableProperties props = null)
        {
        }

        public byte[] FromBytes(byte[] bytes)
        {
            return bytes;
        }
    }
}