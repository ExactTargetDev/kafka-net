namespace Kafka.Client.Common.Imported
{
    public interface IIterator<out TValue>
    {
        bool HasNext();

        TValue Next();
    }
}