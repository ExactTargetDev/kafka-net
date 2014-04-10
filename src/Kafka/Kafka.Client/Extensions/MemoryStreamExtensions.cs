namespace Kafka.Client.Extensions
{
    using System.IO;
    using System.Net;

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

             var value = buffer[0] | buffer[1] << 8 | buffer[2] << 16 | buffer[3] << 24;

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
            var buffer = new byte[8];

            var read = stream.Read(buffer, 0, 8);
            if (read != 8)
            {
                throw new InvalidDataException("Expected 8 bytes in stream, got: " + read);
            }
            var lo = (uint)(buffer[0] | buffer[1] << 8 |
                             buffer[2] << 16 | buffer[3] << 24);
            var hi = (uint)(buffer[4] | buffer[5] << 8 |
                             buffer[6] << 16 | buffer[7] << 24);
            return IPAddress.NetworkToHostOrder((long)((ulong)hi) << 32 | lo);

        }

        public static void PutShort(this MemoryStream stream, short value)
        {
            value = IPAddress.HostToNetworkOrder(value);

            stream.WriteByte((byte)value);
            stream.WriteByte((byte)(value >> 8));
        }

        public static int GetInt(this MemoryStream stream, int index)
        {
            var buffer = stream.GetBuffer();

            var value = buffer[index + 0] | buffer[index + 1] << 8 | buffer[index + 2] << 16 | buffer[index + 3] << 24;

            return IPAddress.NetworkToHostOrder(value);
        }

        public static void PutInt(this MemoryStream stream, int value)
        {
            var buffer = new byte[4];
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
            value = IPAddress.HostToNetworkOrder(value);
            var buffer = stream.GetBuffer();
            buffer[index] = (byte)value;
            buffer[index + 1] = (byte)(value >> 8);
            buffer[index + 2] = (byte)(value >> 16);
            buffer[index + 3] = (byte)(value >> 24);
        }
    }
}