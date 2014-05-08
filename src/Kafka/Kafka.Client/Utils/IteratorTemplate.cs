namespace Kafka.Client.Utils
{
    using System;

    using Kafka.Client.Common.Imported;

    public abstract class IteratorTemplate<T> : IIterator<T> where T : class
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
            if (this.nextItem == null)
            {
                throw new InvalidOperationException("Expected item but none found.");
            }

            return this.nextItem;
        }

        public T Peek()
        {
            if (!this.HasNext())
            {
                throw new InvalidOperationException();
            }

            return this.nextItem;
        }

        public bool HasNext()
        {
            if (this.state == State.Failed)
            {
                throw new InvalidOperationException("Iterator is in failed state");
            }

            switch (this.state)
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
            this.state = State.Failed;
            this.nextItem = this.MakeNext();
            if (this.state == State.Done)
            {
                return false;
            }
            else
            {
                this.state = State.Ready;
                return true;
            }
        }

        protected T AllDone()
        {
            this.state = State.Done;
            return default(T);
        }

        protected void ResetState()
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