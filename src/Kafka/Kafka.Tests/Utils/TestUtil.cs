namespace Kafka.Tests.Utils
{
    using System.Collections.Generic;
    using System.IO;
    using System.Text;

    using Kafka.Client.Common.Imported;

    public class TestUtil
    {
        public static List<T> EnumeratorToArray<T>(IEnumerator<T> enumerator)
        {
            var result = new List<T>();
            while (enumerator.MoveNext())
            {
                result.Add(enumerator.Current);
            }
            return result;
        }

        public static List<T> IteratorToArray<T>(IIterator<T> enumerator)
        {
            var result = new List<T>();
            while (enumerator.HasNext())
            {
                result.Add(enumerator.Next());
            }
            return result;
        }
    }
}