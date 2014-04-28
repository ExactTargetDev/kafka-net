namespace Kafka.Client.Common.Imported
{
    public interface IIterable<T>
    {
        IIterator<T> Iterator();
    }
}