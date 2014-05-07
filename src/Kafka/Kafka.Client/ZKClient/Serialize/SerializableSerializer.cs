namespace Kafka.Client.ZKClient.Serialize
{
    using System;
    using System.Runtime.Serialization;

    public class SerializableSerializer : IZkSerializer
    {
        public byte[] Serialize(object data)
        {
            throw new NotImplementedException();
        }

        public object Deserialize(byte[] bytes)
        {
            throw new NotImplementedException();
        }
    }
}