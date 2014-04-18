namespace Kafka.Client.ZKClient.Serialize
{
    using System.Runtime.Serialization;

    public class SerializableSerializer : IZkSerializer
    {
        public byte[] Serialize(object data)
        {
            throw new System.NotImplementedException();
        }

        public object Deserialize(byte[] bytes)
        {
            throw new System.NotImplementedException();
        }
    }
}