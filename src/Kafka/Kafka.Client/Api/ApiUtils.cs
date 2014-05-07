namespace Kafka.Client.Api
{
    using System;
    using System.Text;

    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;

    /// <summary>
    /// Helper functions specific to parsing or serializing requests and responses
    /// </summary>
    internal class ApiUtils
    {
        public const string ProtocolEncoding = "UTF-8";

        /// <summary>
        /// Read size prefixed string where the size is stored as a 2 byte short.
        /// </summary>
        /// <param name="buffer">The buffer to read from</param>
        /// <returns></returns>
        public static string ReadShortString(ByteBuffer buffer)
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

        /// <summary>
        /// Write a size prefixed string where the size is stored as a 2 byte short
        /// </summary>
        /// <param name="buffer">The buffer to write to</param>
        /// <param name="string">The string to write</param>
        public static void WriteShortString(ByteBuffer buffer, string @string)
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

                buffer.PutShort((short)encodedString.Length);
                buffer.Write(encodedString, 0, encodedString.Length);
            }
        }

        /// <summary>
        /// Return size of a size prefixed string where the size is stored as a 2 byte short
        /// </summary>
        /// <param name="string">The string to write</param>
        /// <returns></returns>
        public static int ShortStringLength(string @string)
        {
            if (@string == null)
            {
                return 2;
            }

            var encodedString = Encoding.UTF8.GetBytes(@string);
            if (encodedString.Length > short.MaxValue)
            {
                throw new KafkaException(string.Format("String exceeds the maximum size of {0}", short.MaxValue));
            }

            return 2 + encodedString.Length;
        }

        /// <summary>
        ///  Read an integer out of the ByteBuffer from the current position and check that it falls within the given
        /// range. If not, throw KafkaException.
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="name"></param>
        /// <param name="range"></param>
        /// <returns></returns>
        public static int ReadIntInRange(ByteBuffer buffer, string name, Tuple<int, int> range)
        {
            var value = buffer.GetInt();
            if (value < range.Item1 || value > range.Item2)
            {
                throw new KafkaException(string.Format("{0} has value {1} which is not in the range {2}", name, value, range));
            }

            return value;
        }

        /// <summary>
        ///  Read an short out of the ByteBuffer from the current position and check that it falls within the given
        /// range. If not, throw KafkaException.
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="name"></param>
        /// <param name="range"></param>
        /// <returns></returns>
        public static short ReadShortInRange(ByteBuffer buffer, string name, Tuple<short, short> range)
        {
            var value = buffer.GetShort();
            if (value < range.Item1 || value > range.Item2)
            {
                throw new KafkaException(string.Format("{0} has value {1} which is not in the range {2}", name, value, range));
            }

            return value;
        }

        /// <summary>
        ///  Read an long out of the ByteBuffer from the current position and check that it falls within the given
        /// range. If not, throw KafkaException.
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="name"></param>
        /// <param name="range"></param>
        /// <returns></returns>
        public static long ReadLongInRange(ByteBuffer buffer, string name, Tuple<long, long> range)
        {
            var value = buffer.GetLong();
            if (value < range.Item1 || value > range.Item2)
            {
                throw new KafkaException(string.Format("{0} has value {1} which is not in the range {2}", name, value, range));
            }

            return value;
        }
    }
}