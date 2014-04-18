namespace Kafka.Client.Utils
{
    using System.Text;

    using Kafka.Client.ZKClient.Serialize;

    public class ZkStringSerializer : IZkSerializer
    {
        public byte[] Serialize(object data)
        {
            return Encoding.UTF8.GetBytes(data.ToString());
        }

        public object Deserialize(byte[] bytes)
        {
            if (bytes == null)
            {
                return null;
            }
            return Encoding.UTF8.GetString(bytes);
        }
    }
}