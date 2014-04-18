namespace Kafka.Client.Utils
{
    using System;
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;

    public class ConcurrentHashSet<T> : IEnumerable<T>
    {
        public class DebugProxy
        {
            private ConcurrentHashSet<T> parent;

            public DebugProxy(ConcurrentHashSet<T> parent)
            {
                this.parent = parent;
            }

            [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
            public object[] Items
            {
                get { return parent.Cast<object>().ToArray(); }
            }
        }

        private readonly ConcurrentDictionary<T, object> inner;

        public ConcurrentHashSet()
        {
            inner = new ConcurrentDictionary<T, object>();
        }

        public ConcurrentHashSet(IEqualityComparer<T> comparer)
        {
            inner = new ConcurrentDictionary<T, object>(comparer);
        }

        public int Count
        {
            get { return inner.Count; }
        }

        public void Add(T item)
        {
            TryAdd(item);
        }

        public void Clear()
        {
            inner.Clear();
        }

        public bool TryAdd(T item)
        {
            return inner.TryAdd(item, null);
        }

        public bool Contains(T item)
        {
            return inner.ContainsKey(item);
        }

        public bool TryRemove(T item)
        {
            object _;
            return inner.TryRemove(item, out _);
        }

        public IEnumerator<T> GetEnumerator()
        {
            return inner.Keys.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}