namespace Kafka.Client.Common.Imported
{
    using System.Threading;

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

        public int GetAndIncrement()
        {
            return Interlocked.Increment(ref this.value);
        }
    }
}