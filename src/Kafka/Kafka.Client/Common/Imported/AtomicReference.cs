namespace Kafka.Client.Common.Imported
{
    using System.Threading;

    public class AtomicReference<T> where T : class
    {
        private T value;

        public void Set(T value)
        {
            Interlocked.Exchange(ref this.value, value);
        }

        public AtomicReference(T initial)
        {
            this.value = initial;
        } 

        public AtomicReference()
        {
            this.value = default(T);
        } 

        public T Get()
        {
            return this.value;
        }
    }
}