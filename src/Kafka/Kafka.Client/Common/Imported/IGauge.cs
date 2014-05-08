namespace Kafka.Client.Common.Imported
{
    public interface IGauge<T>
    {
        T Value { get; }
    }
}