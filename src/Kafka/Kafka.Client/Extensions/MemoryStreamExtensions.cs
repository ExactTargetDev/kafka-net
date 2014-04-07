using System.IO;

namespace Kafka.Client.Extensions
{
    using System;

    using Kafka.Client.Serializers;

    public static class MemoryStreamExtensions
    {
         public static int GetInt(this MemoryStream stream)
         {
             using (var reader = new KafkaBinaryReader(stream))
             {
                 return reader.ReadInt32();
             }
         }

        public static short GetShort(this MemoryStream stream)
        {
            using (var reader = new KafkaBinaryReader(stream))
            {
                return reader.ReadInt16();
            }
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
            using (var writer = new KafkaBinaryWriter(stream))
            {
                writer.Write(value);
            }
        }

        public static int GetInt(this MemoryStream stream, int index)
        {
           throw new NotImplementedException(); //TODO:
        }

        public static void PutInt(this MemoryStream stream, int value)
        {
            using (var writer = new KafkaBinaryWriter(stream))
            {
                writer.Write(value);
            }
        }

        public static void PutLong(this MemoryStream stream, long value)
        {
            using (var writer = new KafkaBinaryWriter(stream))
            {
                writer.Write(value);
            }
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