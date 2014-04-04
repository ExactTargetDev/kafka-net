using System.Text;
using Kafka.Client.Utils;

namespace Kafka.Client.Serializers
{
    public class StringDecoder : IDecoder<string>
    {
        private readonly string encoding;

        public StringDecoder(VerifiableProperties props = null)
         {
             encoding = (props == null) ? "UTF8" : props.GetString("serializer.config", "UTF8");
         }

        public string FromBytes(byte[] bytes)
        {
            return Encoding.GetEncoding(encoding).GetString(bytes);
        }
    }
}