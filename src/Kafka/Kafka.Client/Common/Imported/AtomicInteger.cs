namespace Kafka.Client.Common.Imported
{
    using System.Threading;

    public class AtomicInteger
    {
        private int value;

        public AtomicInteger(int initialValue)
        {
            this.value = initialValue;
        }

        public AtomicInteger()
            : this(0)
        {
        }

        public long Get()
        {
            return this.value;
        } 

        public int GetAndIncrement()
        {
            return Interlocked.Increment(ref this.value);
        }
    }
}