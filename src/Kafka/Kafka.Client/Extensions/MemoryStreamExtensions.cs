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

        public static void PutInt(this MemoryStream stream, int index, int value)
        {
            throw new NotImplementedException();//TODO:
        }
    }
}