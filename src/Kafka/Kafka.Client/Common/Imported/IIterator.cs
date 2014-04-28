namespace Kafka.Client.Common.Imported
{
    public interface IIterator<TValue>
    {
        bool HasNext();

        TValue Next();
    }
}