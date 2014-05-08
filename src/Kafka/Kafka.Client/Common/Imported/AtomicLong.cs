namespace Kafka.Client.Common.Imported
{
    using System.Threading;

    public class AtomicLong
    {
        private long value;

        public AtomicLong(long initialValue)
        {
            this.value = initialValue;
        }

        public AtomicLong() : this(0)
        {
        }

        public long Get()
        {
            return Interlocked.Read(ref this.value);
        }

        public void Set(long newValue)
        {
            Interlocked.Exchange(ref this.value, newValue);
        }

        public long GetAndIncrement()
        {
            return Interlocked.Increment(ref this.value) - 1;
        }

        public override string ToString()
        {
            return string.Format("{0}", this.value);
        }
    }
}