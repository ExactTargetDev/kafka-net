namespace Kafka.Client.Common.Imported
{
    public class AtomicInteger
    {
        private int value;

        public AtomicInteger(int initialValue)
        {
            value = initialValue;
        }

        public AtomicInteger()
            : this(0)
        {
        }

        public long Get()
        {
            return value;
        } 
    }
}