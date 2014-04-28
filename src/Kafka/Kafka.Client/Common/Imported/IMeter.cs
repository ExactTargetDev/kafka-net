namespace Kafka.Client.Common.Imported
{
    public interface IMeter
    {
        void Mark();

        void Mark(int value);
    }
}