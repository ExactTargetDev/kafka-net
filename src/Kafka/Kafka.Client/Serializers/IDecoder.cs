namespace Kafka.Client.Serializers
{
    /// <summary>
    /// A decoder is a method of turning byte arrays into objects.
    /// An implementation is required to provide a constructor that
    /// takes a VerifiableProperties instance.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IDecoder<T>
    {
        T FromBytes(byte[] bytes);  
    }
}