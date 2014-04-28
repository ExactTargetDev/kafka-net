using System.Collections.Generic;

namespace Kafka.Client.Utils
{
    using System;
    using System.Collections;

    using Kafka.Client.Common.Imported;

    public abstract class IteratorTemplate<T> : IIterator<T>
    {

        private State state = State.NotReady;

        private T nextItem;

        public virtual T Next()
        {
            if (!this.HasNext())
            {
                throw new InvalidOperationException();
            }
            this.state = State.NotReady;
            if (nextItem == null)
            {
                throw new InvalidOperationException("Expected item but none found.");
            }
            return nextItem;
        }

        public T Peek()
        {
            if (!HasNext())
            {
                throw new InvalidOperationException();
            }
            return this.nextItem;
        }

        public bool HasNext()
        {
            if (state == State.Failed)
            {
                throw new InvalidOperationException("Iterator is in failed state");
            }
            switch (state)
            {
                case State.Done:
                    return false;
                case State.Ready:
                    return true;
                default:
                    return this.MaybeComputeNext();
            }
        }

        protected abstract T MakeNext();

        internal bool MaybeComputeNext()
        {
            state = State.Failed;
            nextItem = this.MakeNext();
            if (state == State.Done)
            {
                return false;
            }
            else
            {
                state = State.Ready;
                return true;
            }
        }

        protected T AllDone()
        {
            state = State.Done;
            return default(T);
        }

        protected void ResetState()
        {
            state = State.NotReady;
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