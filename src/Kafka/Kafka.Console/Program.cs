using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Console
{
    using System.Diagnostics;
    using System.IO;
    using System.Threading;

    using Kafka.Client.Api;
    using Kafka.Client.Common.Imported;
    using Kafka.Tests;
    using Kafka.Tests.Consumers;
    using Kafka.Tests.Utils;

    using Console = System.Console;

    class Program
    {
        static void Main(string[] args)
        {

            new ZookeeperConsumerConnectorTest().TestBasic();

        }

       

        public static byte[] StringToByteArray(string hex)
        {
            return Enumerable.Range(0, hex.Length)
                             .Where(x => x % 2 == 0)
                             .Select(x => Convert.ToByte(hex.Substring(x, 2), 16))
                             .ToArray();
        }
    }
}
