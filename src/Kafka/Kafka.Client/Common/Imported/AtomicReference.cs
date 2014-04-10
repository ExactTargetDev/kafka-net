namespace Kafka.Client.Common.Imported
{
    using System.Threading;

    public class AtomicReference<T> where T : class
    {
        private T value;

        public void Set(T newValue)
        {
            Interlocked.Exchange(ref this.value, newValue);
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