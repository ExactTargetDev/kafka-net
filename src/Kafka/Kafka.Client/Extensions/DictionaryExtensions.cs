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
    }
}