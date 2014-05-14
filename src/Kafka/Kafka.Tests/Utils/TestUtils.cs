namespace Kafka.Tests.Utils
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;

    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Messages;
    using Kafka.Client.Utils;
    using Kafka.Tests.Custom.Server;

    using Xunit;

    public static class TestUtils
    {
        private static readonly string IoTmpDir = Path.GetTempPath();

        private const string Letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

        private const string Digits = "0123456789";

        private const string LettersAndDigits = Letters + Digits;

        // A consistent random number generator to make tests repeatable
        private static Random seededRandom = new Random(192348092);

        private static Random random = new Random();

        private static readonly ISet<int> AvailablePorts = new HashSet<int>(Enumerable.Range(2100, 2200));

        public static void PortReleased(int port)
        {
            AvailablePorts.Add(port);
        }

        public static List<int> ChoosePorts(int count)
        {
            if (AvailablePorts.Count < count)
            {
                throw new KafkaException("Unable to get " + count + " ports as only " + AvailablePorts.Count + " left");
            }
            return Enumerable.Range(1, count).Select(idx =>
                {
                    var randomPort = AvailablePorts.ToArray()[random.Next(AvailablePorts.Count)];
                    AvailablePorts.Remove(randomPort);
                    return randomPort;
                }).ToList();
        }

        public static int ChoosePort()
        {
            return ChoosePorts(1).First();
        }

        public static DirectoryInfo TempDir()
        {
            return Directory.CreateDirectory(Path.Combine(IoTmpDir, "kafka-dir-" + random.Next(1000000)));
        }

        public static string TempFile()
        {
            while (true)
            {
                var path = Path.Combine(IoTmpDir, "kafka-" + random.Next(1000000));
                if (!File.Exists(path))
                {
                    File.WriteAllText(path, string.Empty);
                    return path;
                }
            }
        }

        public static List<TempKafkaConfig> CreateBrokerConfigs(int numConfigs, Func<int, Dictionary<string, string>> customProps)
        {
            return ChoosePorts(numConfigs).Select((port, node) => CreateBrokerConfig(node, port, customProps)).ToList();
        }


        public static TempKafkaConfig CreateBrokerConfig(
            int nodeId, int port, Func<int, Dictionary<string, string>> customProps)
        {
            var props = new Dictionary<string, string>
                            {
                                { "broker.id", nodeId.ToString() },
                                { "host.name", "localhost" },
                                { "port", port.ToString() },
                                { "log.dir", TempDir().FullName },
                                { "zookeeper.connect", TestZkUtils.ZookeeperConnect },
                                { "replica.socket.timeout.ms", "1500" }
                            };
            var overrides = customProps(nodeId);
            foreach (var kvp in overrides)
            {
                props[kvp.Key] = kvp.Value;
            }
            return TempKafkaConfig.Create(props);
        }


        class MessageIterator : IteratorTemplate<Message>
        {
            private IIterator<MessageAndOffset> iter;

            public MessageIterator(IIterator<MessageAndOffset> iter)
            {
                this.iter = iter;
            }

            protected override Message MakeNext()
            {
                if (this.iter.HasNext())
                {
                    return iter.Next().Message;
                }
                return this.AllDone();
            }
        }

         public static IIterator<Message> GetMessageIterator(IIterator<MessageAndOffset> iter)
         {
             return new MessageIterator(iter);
         }

        public static string RandomString(int len)
        {
            var b = new StringBuilder();
            for (int i = 0; i < len; i++)
            {
                b.Append(LettersAndDigits.ToCharArray()[seededRandom.Next(LettersAndDigits.Length)]);
            }
            return b.ToString();
        }


        /// <summary>
         /// Throw an exception if the two iterators are of differing lengths or contain
         /// different messages on their Nth element
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="expected"></param>
        /// <param name="actual"></param>
        public static void CheckEquals<T>(IEnumerator<T> expected, IEnumerator<T> actual)
        {
            var length = 0;
            while (expected.MoveNext() && actual.MoveNext())
            {
                length++;
                Assert.Equal(expected.Current, actual.Current);
            }

            // check if the expected iterator is longer
            if (expected.MoveNext())
            {
                var length1 = length;
                while (expected.MoveNext())
                {
                    var current = expected.Current;
                    length1++;
                }
                Assert.False(false, "Iterators have uneven length -- first has more " + length1 + " > " + length);
            }

            // check if the actual iterator was longer
            if (actual.MoveNext())
            {
                var length2 = length;
                while (actual.MoveNext())
                {
                    var current = actual.Current;
                    length2++;
                }
                Assert.False(false, "Iterators have uneven length -- second has more " + length2 + " > " + length);
            }

        }
    }

    internal static class TestZkUtils
    {
        public static string ZookeeperConnect { get; private set; }

        public static int ZookeeperPort { get; private set; }

        static TestZkUtils()
        {
            ZookeeperPort = TestUtils.ChoosePort();
            ZookeeperConnect = "127.0.0.1:" + ZookeeperPort;
        }
    }
}