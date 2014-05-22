namespace Kafka.Tests.Utils
{
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using System.Threading;

    using Kafka.Client.Common.Imported;

    using Xunit;

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

        public static void DeleteFile(string path)
        {
            if (path != null)
            {
                SpinWait.SpinUntil(
                    () =>
                        {
                            try
                            {
                                File.Delete(path);
                            }
                            catch
                            {
                            }
                            return !File.Exists(path);
                        },
                    1000);
                Assert.False(File.Exists(path));
            }
        }

        public static void DeleteDir(string path)
        {
            if (path != null)
            {
                Directory.Delete(path, true);
                Assert.False(Directory.Exists(path));
            }
        }
    }
}