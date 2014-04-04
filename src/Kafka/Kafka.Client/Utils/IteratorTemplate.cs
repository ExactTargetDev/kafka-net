using System.Collections.Generic;

namespace Kafka.Client.Utils
{
    using System;
    using System.Collections;

    internal abstract class IteratorTemplate<T> : IEnumerator<T>
    {
         
        private State state = State.NotReady;

        private T nextItem;

        object IEnumerator.Current 
        {
            get
            {
                return this.nextItem;
            } 
        }

        public T Current 
        { 
            get
            {
                return this.nextItem;
            }
        }

        public void Dispose()
        {
            state = State.Done;
        }

        public bool MoveNext()
        {
            if (this.state == State.Failed)
            {
                throw new InvalidOperationException("Iterator is in failed state");
            }
            if (this.state == State.Done)
            {
                return false;
            }
            return this.MaybeComputeNext();
        }

        private bool MaybeComputeNext()
        {
            this.state = State.Failed;
            this.nextItem = this.MakeNext();
            if (state == State.Done)
            {
                return false;
            }
            this.state = State.Ready;


            throw new NotImplementedException();
        }

        protected T AllDone()
        {
            this.state = State.Done;
            return default(T);
        }

        protected abstract T MakeNext();

        public void Reset()
        {
            this.state = State.NotReady;
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