#region License

/*
 * Copyright (C) 2002-2010 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#endregion

namespace Kafka.Client.Locks
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    using Kafka.Client.Locks.Helpers;

    /// <author>Doug Lea</author>
    /// <author>Griffin Caprio (.NET)</author>
    /// <author>Kenneth Xu</author>
    [Serializable]
    internal class FIFOConditionVariable : ConditionVariable // BACKPORT_3_1
    {
        private static readonly IQueuedSync _sync = new Sync();

        private readonly IWaitQueue _wq = new FIFOWaitQueue();

        private class Sync : IQueuedSync
        {
            public bool Recheck(WaitNode node) { return false; }
            public void TakeOver(WaitNode node) { }
        }

        protected internal override int WaitQueueLength
        {
            get
            {
                this.AssertOwnership();
                return this._wq.Length;
            }

        }

        protected internal override ICollection<Thread> WaitingThreads
        {
            get
            {
                this.AssertOwnership();
                return this._wq.WaitingThreads;
            }

        }

        /// <summary>
        /// Create a new <see cref="FIFOConditionVariable"/> that relies on the
        /// given mutual exclusion lock.
        /// </summary>
        /// <param name="lock">A non-reentrant mutual exclusion lock.</param>
        internal FIFOConditionVariable(IExclusiveLock @lock)
            : base(@lock)
        {
        }

        public override void AwaitUninterruptibly()
        {
            this.DoWait(n => n.DoWaitUninterruptibly(_sync));
        }

        public override void Await()
        {
            this.DoWait(n => n.DoWait(_sync));
        }

        public override bool Await(TimeSpan timespan)
        {
            bool success = false;
            this.DoWait(n => success = n.DoTimedWait(_sync, timespan));
            return success;
        }

        public override bool AwaitUntil(DateTime deadline)
        {
            return this.Await(deadline.Subtract(DateTime.UtcNow));
        }

        public override void Signal()
        {
            this.AssertOwnership();
            for (; ; )
            {
                WaitNode w = this._wq.Dequeue();
                if (w == null) return;  // no one to signal
                if (w.Signal(_sync)) return; // notify if still waiting, else skip
            }
        }

        public override void SignalAll()
        {
            this.AssertOwnership();
            for (; ; )
            {
                WaitNode w = this._wq.Dequeue();
                if (w == null) return;  // no more to signal
                w.Signal(_sync);
            }
        }

        protected internal override bool HasWaiters
        {
            get
            {
                this.AssertOwnership();
                return this._wq.HasNodes;
            }
        }

        private void DoWait(Action<WaitNode> action)
        {
            int holdCount = this.Lock.HoldCount;
            if (holdCount == 0)
            {
                throw new SynchronizationLockException();
            }
            WaitNode n = new WaitNode();
            this._wq.Enqueue(n);
            for (int i = holdCount; i > 0; i--) this.Lock.Unlock();
            try
            {
                action(n);
            }
            finally
            {
                for (int i = holdCount; i > 0; i--) this.Lock.Lock();
            }
        }
    }
}