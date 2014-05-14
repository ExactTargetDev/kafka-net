namespace Kafka.Client.Extensions
{
    using System.Collections.Generic;
    using System.Linq;

    public static class DictionaryExtensions
    {
        public static string DictionaryToString<TKey, TValue>(this IDictionary<TKey, TValue> hash)
        {
            return string.Join(", ", hash.Select(kvp => kvp.Key + ":" + kvp.Value));
        }

        public static bool DictionaryEqual<TKey, TValue>(
            this IDictionary<TKey, TValue> first, IDictionary<TKey, TValue> second)
        {
            if (first == second) return true;
            if ((first == null) || (second == null)) return false;
            if (first.Count != second.Count) return false;

            var comparer = EqualityComparer<TValue>.Default;

            foreach (KeyValuePair<TKey, TValue> kvp in first)
            {
                TValue secondValue;
                if (!second.TryGetValue(kvp.Key, out secondValue)) return false;
                if (!comparer.Equals(kvp.Value, secondValue)) return false;
            }
            return true;
        }


    }
}