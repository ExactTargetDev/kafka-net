namespace Kafka.Client.Serializers
{
    using System.Text;

    using Kafka.Client.Utils;

    public class StringDecoder : IDecoder<string>
    {
        private readonly string encoding;

        public StringDecoder()
        {
            this.encoding = "UTF-8";
        }

        public string FromBytes(byte[] bytes)
        {
            return Encoding.GetEncoding(this.encoding).GetString(bytes);
        }
    }
}