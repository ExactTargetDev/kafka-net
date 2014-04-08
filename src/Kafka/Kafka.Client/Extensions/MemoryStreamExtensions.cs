using System.IO;

namespace Kafka.Client.Extensions
{
    using System;
    using System.Net;

    using Kafka.Client.Serializers;

    public static class MemoryStreamExtensions
    {
         public static int GetInt(this MemoryStream stream)
         {
             var buffer = new byte[4];

             var read = stream.Read(buffer, 0, 4);
             if (read != 4)
             {
                 throw new InvalidDataException("Expected 4 bytes in stream, got: " + read);
             }

             var value = (int)(buffer[0] | buffer[1] << 8 | buffer[2] << 16 | buffer[3] << 24);

             return IPAddress.NetworkToHostOrder(value);
         }

        public static short GetShort(this MemoryStream stream)
        {
            var buffer = new byte[2];

            var read = stream.Read(buffer, 0, 2);
            if (read != 2)
            {
                throw new InvalidDataException("Expected 2 bytes in stream, got: " + read);
            }

            var value = (short)(buffer[0] | buffer[1] << 8);

            return IPAddress.NetworkToHostOrder(value);
        }

        public static long GetLong(this MemoryStream stream)
        {
            using (var reader = new KafkaBinaryReader(stream))
            {
                return reader.ReadInt64();
            }
        }

        public static void PutShort(this MemoryStream stream, short value)
        {
            value = IPAddress.HostToNetworkOrder(value);

            stream.WriteByte((byte) value);
            stream.WriteByte((byte)(value >> 8));
        }

        public static int GetInt(this MemoryStream stream, int index)
        {
           throw new NotImplementedException(); //TODO:
        }

        public static void PutInt(this MemoryStream stream, int value)
        {
            var buffer= new byte[4];
            value = IPAddress.HostToNetworkOrder(value);

            buffer[0] = (byte)value;
            buffer[1] = (byte)(value >> 8);
            buffer[2] = (byte)(value >> 16);
            buffer[3] = (byte)(value >> 24);

            stream.Write(buffer, 0, 4);
        }

        public static void PutLong(this MemoryStream stream, long value)
        {
            var buffer = new byte[8];
            value = IPAddress.HostToNetworkOrder(value);

            buffer[0] = (byte)value;
            buffer[1] = (byte)(value >> 8);
            buffer[2] = (byte)(value >> 16);
            buffer[3] = (byte)(value >> 24);
            buffer[4] = (byte)(value >> 32);
            buffer[5] = (byte)(value >> 40);
            buffer[6] = (byte)(value >> 48);
            buffer[7] = (byte)(value >> 56);

            stream.Write(buffer, 0, 8);
        }

        public static void PutInt(this MemoryStream stream, int index, int value)
        {
            byte[] buffer = stream.GetBuffer();
            buffer[index] = (byte)value;
            buffer[index + 1] = (byte)(value >> 8);
            buffer[index + 2] = (byte)(value >> 16);
            buffer[index + 3] = (byte)(value >> 24);
            buffer[index + 4] = (byte)(value >> 32);
            buffer[index + 5] = (byte)(value >> 40);
            buffer[index + 6] = (byte)(value >> 48);
            buffer[index + 7] = (byte)(value >> 56);

        }
    }
}