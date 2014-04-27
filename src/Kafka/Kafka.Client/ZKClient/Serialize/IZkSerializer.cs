namespace Kafka.Client.ZKClient.Serialize
{
    /// <summary>
    /// Zookeeper is able to store Data in form of byte arrays. This interfacte is a bridge between those byte-array format
    /// and higher level objects.
    /// </summary>
    public interface IZkSerializer
    {
        byte[] Serialize(object data);

        object Deserialize(byte[] bytes);
    }
}