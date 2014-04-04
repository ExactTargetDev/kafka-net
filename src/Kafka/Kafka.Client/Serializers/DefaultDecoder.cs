using Kafka.Client.Utils;

namespace Kafka.Client.Serializers
{
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