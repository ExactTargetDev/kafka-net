namespace Kafka.Client.Serializers
{
    public class DefaultDecoder : IDecoder<byte[]>
    {
        public byte[] FromBytes(byte[] bytes)
        {
            return bytes;
        }
    }
}