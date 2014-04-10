namespace Kafka.Client.Serializers
{
    using System.Text;

    using Kafka.Client.Utils;

    public class StringDecoder : IDecoder<string>
    {
        private readonly string encoding;

        public StringDecoder(VerifiableProperties props = null)
         {
             this.encoding = (props == null) ? "UTF8" : props.GetString("serializer.encoding", "UTF8");
         }

        public string FromBytes(byte[] bytes)
        {
            return Encoding.GetEncoding(this.encoding).GetString(bytes);
        }
    }
}