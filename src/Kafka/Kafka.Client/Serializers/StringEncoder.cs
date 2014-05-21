namespace Kafka.Client.Serializers
{
    using System.Text;

    using Kafka.Client.Cfg;
    using Kafka.Client.Producers;
    using Kafka.Client.Utils;

    public class StringEncoder : IEncoder<string>
    {
          private readonly string encoding;

         public StringEncoder(ProducerConfig config = null)
         {
             this.encoding = "UTF-8";
         }

        public byte[] ToBytes(string t)
        {
            if (t == null)
            {
                return null;
            }
            else
            {
                return Encoding.GetEncoding(this.encoding).GetBytes(t);    
            }
        }
    }
}