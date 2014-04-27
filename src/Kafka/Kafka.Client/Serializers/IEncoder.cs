namespace Kafka.Client.Serializers
{
    /// <summary>
    /// An encoder is a method of turning objects into byte arrays.
    /// An implementation is required to provide a constructor that
    /// takes a VerifiableProperties instance.
    /// </summary>
    /// <typeparam name="T">
    /// Type od Data
    /// </typeparam>
    public interface IEncoder<T>
    {

        byte[] ToBytes(T t);
    }
}