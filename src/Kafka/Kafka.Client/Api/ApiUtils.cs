namespace Kafka.Client.Api
{
    using System.IO;
    using System.Text;

    using Kafka.Client.Common;
    using Kafka.Client.Extensions;

    public class ApiUtils
    {
        public const string protocolEncoding = "UTF-8";

        /// <summary>
        /// Read size prefixed string where the size is stored as a 2 byte short.
        /// </summary>
        /// <param name="buffer"></param>
        /// <returns></returns>
        public static string ReadShortString(MemoryStream buffer)
        {
            var size = buffer.GetShort();
            if (size < 0)
            {
                return null;
            }
            var bytes = new byte[size];
            buffer.Read(bytes, 0, bytes.Length);
            return Encoding.UTF8.GetString(bytes);
        }

        public static void WriteShortString(MemoryStream buffer, string @string)
        {
            if (@string == null)
            {
                buffer.PutShort(-1);
            }
            else
            {
                var encodedString = Encoding.UTF8.GetBytes(@string);
                if (encodedString.Length > short.MaxValue)
                {
                    throw new KafkaException(string.Format("String exceeds the maximum size of {0}", short.MaxValue));
                }
                else
                {
                    buffer.PutShort((short)encodedString.Length);
                    buffer.Write(encodedString, 0, encodedString.Length);
                }
            }
        }

        public static int ShortStringLength(string @string)
        {
            if (@string == null)
            {
                return 2;
            }
            else
            {
                var encodedString = Encoding.UTF8.GetBytes(@string);
                if (encodedString.Length > short.MaxValue)
                {
                    throw new KafkaException(string.Format("String exceeds the maximum size of {0}", short.MaxValue));
                }
                else
                {
                    return 2 + encodedString.Length;
                }
            }
        }

        //TODO: finish me
    }
}