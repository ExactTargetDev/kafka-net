using System.Collections.Generic;

namespace Kafka.Client.Utils
{
    using System.Collections;

    internal class IteratorTemplate<T> : IEnumerator<T>
    {
         
        private State state = State.NotReady;
        private T nextItem = default (T);

        public T Current { get; private set; }

        object IEnumerator.Current 
        {
            get
            {
                return this.Current;
            } 
        }

        public void Dispose()
        {
            throw new System.NotImplementedException();
        }

        public bool MoveNext()
        {
            throw new System.NotImplementedException();
        }

        public void Reset()
        {
            throw new System.NotImplementedException();
        }
    }


    internal enum State
    {
        Done,
        Ready,
        NotReady,
        Failed
    }

}