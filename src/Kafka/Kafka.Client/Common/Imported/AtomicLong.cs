using System.Threading;

namespace Kafka.Client.Common.Imported
{
    public class AtomicLong
    {
        private long value;

        public AtomicLong(long initialValue)
        {
            value = initialValue;
        }

        public AtomicLong() : this(0)
        {
        }

        public long Get()
        {
            return Interlocked.Read(ref value);
        }

        public void Set(long newValue)
        {
            Interlocked.Exchange(ref value, newValue);
        }
    }
}