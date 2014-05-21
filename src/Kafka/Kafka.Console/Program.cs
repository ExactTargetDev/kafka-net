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

    using log4net.Appender;

    using Console = System.Console;

    class Program
    {
        static void Main(string[] args)
        {

            log4net.Config.BasicConfigurator.Configure(new FileAppender
            {
                File = @"c:\tmp\kafka.log",
                Layout = new log4net.Layout.SimpleLayout()
            });
            using (var test = new ZookeeperConsumerConnectorTest())
            {
                test.TestBasic();
            }

            Console.Clear();

            using (var test = new ZookeeperConsumerConnectorTest())
            {
                test.TestCompression();
            }

            Console.Clear();
            //TODO: move to separate class and config statically

           

            using (var test = new ZookeeperConsumerConnectorTest())
            {
                test.TestLeaderSelectionForPartition();
            }
        }
    }
}
