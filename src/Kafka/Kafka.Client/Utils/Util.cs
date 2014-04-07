using System;
using System.IO;
using Kafka.Client.Extensions;

namespace Kafka.Client.Utils
{
    using System.Reflection;

    using Kafka.Client.Producers;

    /// <summary>
    /// Original name: Utils
    /// </summary>
    public static class Util
    {
     /// <summary>
        ///  Read the given byte buffer into a byte array
     /// </summary>
     /// <param name="stream"></param>
     /// <returns></returns>
    
        public static byte[] ReadBytes(MemoryStream stream)
        {
         return stream.ToArray();
        }

        public static byte[] ReadBytes(MemoryStream stream, int offset, int size)
        {
            var result = new byte[size];
            Buffer.BlockCopy(stream.GetBuffer(), offset, result, 0, size);
            return result;
        }

        /// <summary>
        /// Read an unsigned integer from the current position in the buffer, 
        /// incrementing the position by 4 bytes
        /// </summary>
        /// <param name="buffer"></param>
        /// <returns></returns>
        public static long ReadUnsignedInt(MemoryStream buffer)
        {
            return buffer.GetInt() & 0xffffffffL;
        }

        /// <summary>
        /// Read an unsigned integer from the given position without modifying the buffers
        /// position
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static long ReadUnsingedInt(MemoryStream buffer, int index)
        {
            return buffer.GetInt(index) & 0xffffffffL;
        }

        /// <summary>
        /// Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
        /// </summary>
        /// <param name="buffer">The buffer to write to</param>
        /// <param name="value">The value to write</param>
        public static void WriteUnsignedInt(MemoryStream buffer, long value)
        {
            buffer.PutInt((int) (value & 0xffffffffL));
        }

        /// <summary>
        /// Write the given long value as a 4 byte unsigned integer. Overflow is ignored
        /// </summary>
        /// <param name="buffer">The buffer to write to</param>
        /// <param name="index"></param>
        /// <param name="value"></param>
        public static void WriteUnsignedInt(MemoryStream buffer, int index, long value)
        {
            buffer.PutInt(index, (int)(value & 0xffffffffL));
        }


        /// <summary>
        /// Compute the CRC32 of the byte array
        /// </summary>
        /// <param name="bytes">The array to compute the checksum for</param>
        /// <returns>The CRC32</returns>
        public static long Crc32(byte[] bytes)
        {
            return Crc32(bytes, 0, bytes.Length);
        }

        public static long Crc32(byte[] bytes, int offset, int size)
        {
            var crc = new Crc32();
            return BitConverter.ToInt32(crc.ComputeHash(bytes, offset, size), 0);
        }

        public static T CreateObject<T>(string type, params object[] args)
        {
            return (T)Activator.CreateInstance(Type.GetType(type), args);
        }

    }
}