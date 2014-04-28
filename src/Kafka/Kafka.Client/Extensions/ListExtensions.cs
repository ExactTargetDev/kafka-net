namespace Kafka.Client.Extensions
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;

    using Kafka.Client.Common.Imported;

    public static class ListExtensions
    {
        private static readonly Random Rng = new Random();

        public static IList<T> Shuffle<T>(this IList<T> list)
        {
            var n = list.Count;
            while (n > 1)
            {
                n--;
                var k = Rng.Next(n + 1);
                var value = list[k];
                list[k] = list[n];
                list[n] = value;
            }

            return list;
        }

        /// <summary>
        /// Scala version of group by
        /// </summary>
        /// <typeparam name="TGroup"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <returns></returns>
        public static IDictionary<TGroup, IDictionary<TKey, TValue>> GroupByScala<TGroup, TKey, TValue>(
            this IDictionary<TKey, TValue> dict, Func<KeyValuePair<TKey, TValue>, TGroup> keySelector)
        {
            var result = new Dictionary<TGroup, IDictionary<TKey, TValue>>();
            foreach (var kvp in dict)
            {
                var selectedKey = keySelector(kvp);
                if (result.ContainsKey(selectedKey) == false)
                {
                    result[selectedKey] = new Dictionary<TKey, TValue>();
                }

                result[selectedKey][kvp.Key] = kvp.Value;
            }

            return result;
        }

        public static IEnumerable<TElement> ToEnumerable<TElement>(this IIterator<TElement> iterator)
        {
            while (iterator.HasNext())
            {
                yield return iterator.Next();
            }
        }

        public static void Clear<T>(this BlockingCollection<T> blockingCollection)
        {
            if (blockingCollection == null)
            {
                throw new ArgumentNullException("blockingCollection");
            }

            while (blockingCollection.Count > 0)
            {
                T item;
                blockingCollection.TryTake(out item);
            }
        }

        public static TValue Get<TKey, TValue>(this IDictionary<TKey, TValue> self, TKey key) where TKey : class
        {
            TValue result;
            return self.TryGetValue(key, out result) ? result : default(TValue);
        }
    }
}