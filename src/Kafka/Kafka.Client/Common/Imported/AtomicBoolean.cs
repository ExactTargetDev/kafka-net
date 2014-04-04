namespace Kafka.Client.Common.Imported
{
    using System.Threading;

    public class AtomicBoolean
    {
        private int value;

        public AtomicBoolean(bool initialValue)
        {
            this.value = initialValue ? 1 : 0;
        }

        public AtomicBoolean()
            : this(false)
        {
        }

        public bool Get()
        {
            return this.value == 1;
        }  

        public void Set(bool newValue)
        {
            this.value = newValue ? 1 : 0;
        }

        public bool CompareAndSet(bool current, bool newValue)
        {
            return Interlocked.CompareExchange(ref this.value, current ? 1 : 0, newValue ? 1 : 0) == 1;
        }
    }
}