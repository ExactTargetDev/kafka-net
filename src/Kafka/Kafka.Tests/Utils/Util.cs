namespace Kafka.Tests.Utils
{
    using System.Collections.Generic;
    using System.IO;
    using System.Text;

    public class Util
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

        public static string ReadMemoryStream(MemoryStream stream)
        {
            var buffer = new byte[stream.Length];
            stream.Read(buffer, 0, buffer.Length);

            stream.Position = 0;
            return Encoding.UTF8.GetString(buffer);

        }
    }
}