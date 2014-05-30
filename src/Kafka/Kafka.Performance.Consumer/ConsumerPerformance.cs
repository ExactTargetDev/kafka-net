namespace Kafka.Performance.Consumer
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Threading;

    using Kafka.Client.Common.Imported;
    using Kafka.Client.Consumers;
    using Kafka.Client.Utils;
    using Kafka.Performance.Common;

    using log4net;
    using log4net.Appender;
    using log4net.Config;
    using log4net.Layout;

    /// <summary>
    /// Performance test for the full zookeeper consumer
    /// </summary>
    public class ConsumerPerformance
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public static void Main(string[] args)
        {
           //TODO: BasicConfigurator.Configure(new ConsoleAppender(new SimpleLayout()));

            var config = new ConsumerPerfConfig(args);
            Logger.Info("Starting consumer...");
            var totalMessagesRead = new AtomicLong(0);
            var totalBytesRead = new AtomicLong(0);

            if (!config.HideHeader)
            {
                if (!config.ShowDetailedStats)
                {
                    Console.WriteLine(
                        "start.time, end.time, fetch.size, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec");
                }
                else
                {
                    Console.WriteLine("time, fetch.size, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec");
                }
            }

            // clean up zookeeper state for this group id for every perf run
            ZkUtils.MaybeDeletePath(config.ConsumerConfig.ZooKeeper.ZkConnect, "/consumers/" + config.ConsumerConfig.GroupId);

            var consumerConnector = Consumer.Create(config.ConsumerConfig);

            var topicMessageStreams =
                consumerConnector.CreateMessageStreams(
                    new Dictionary<string, int> { { config.Topic, config.NumThreads } });

            var threadList = new List<ConsumerPerfThread>();
            foreach (var topicAndMessageStream in topicMessageStreams)
            {
                var streamList = topicAndMessageStream.Value;
                for (var i = 0; i < streamList.Count; i++)
                {
                    threadList.Add(new ConsumerPerfThread(i, "kafka-zk-consumer-" + i, streamList[i], config, totalMessagesRead, totalBytesRead));
                }

                Logger.Info("Sleeping for 1 second.");
                Thread.Sleep(1000);
                Logger.Info("Starting threads.");
                var startMs = DateTime.Now;
                foreach (var thread in threadList)
                {
                    new Thread(() => thread.Run()).Start();
                }

                foreach (var thread in threadList)
                {
                    thread.Shutdown();
                }

                var endMs = DateTime.Now;
                var elapsedSecs = (endMs - startMs - TimeSpan.FromMilliseconds(config.ConsumerConfig.ConsumerTimeoutMs)).TotalMilliseconds / 1000.0;
                if (!config.ShowDetailedStats)
                {
                    var totalMBRead = (totalBytesRead.Get() * 1.0) / (1024 * 1024);
                    Console.WriteLine(
                        "{0}, {1}, {2}, {3:f4}, {4:f4}, {5}, {6:f4}",
                        startMs.ToString(config.DateFormat),
                         endMs.ToString(config.DateFormat),
                         config.ConsumerConfig.FetchMessageMaxBytes,
                         totalMBRead,
                         totalMBRead / elapsedSecs,
                         totalMessagesRead.Get(),
                         totalMessagesRead.Get() / elapsedSecs);
                }
            }
            Environment.Exit(0);
        }
    }

    internal class ConsumerPerfConfig : PerfConfig
    {
        public string GroupId { get; private set; }

        public int SocketReceiveBufferBytes { get; private set; }

        public int FetchMessageMaxBytes { get; private set; }

        public bool AutoOffsetReset { get; private set; }

        public string ZookeeperConnect { get; private set; }

        public int NumConsumerFetchers { get; private set; }

        public int NumThreads { get; private set; }

        public string Topic { get; private set; }

        public ConsumerConfig ConsumerConfig { get; private set; }

        public ConsumerPerfConfig(string[] args)
        {
            this.Args = args;

            this.GroupId = "perf-consumer" + new Random().Next(100000);
            this.FetchMessageMaxBytes = 1024 * 1024;
            this.AutoOffsetReset = false;
            this.SocketReceiveBufferBytes = 2 * 1024 * 1024;
            this.NumThreads = 10;
            this.NumConsumerFetchers = 1;

            OptionSet.Add(
                "zookeeper=",
                "REQUIRED: The connection string for the zookeeper connection in the form host:port. Multiple URLS can be given to allow fail-over.",
                b => this.ZookeeperConnect = b);

            OptionSet.Add("topic=", "REQUIRED: The topic to consume from.", t => this.Topic = t);

            OptionSet.Add("group:", "The group id to consume on.", g => this.GroupId = g);

            OptionSet.Add(
                "fetch-size:", "The amount of data to fetch in a single request.", f => this.FetchMessageMaxBytes = int.Parse(f));

            OptionSet.Add(
                "from-latest",
                "If the consumer does not already have an established offset to consume from, start with the latest message present in the log rather than the earliest message.",
                _ => this.AutoOffsetReset = true);

            OptionSet.Add(
                "socket-buffer-size:",
                "The size of the tcp RECV size.",
                s => this.SocketReceiveBufferBytes = int.Parse(s));

            OptionSet.Add("threads:", "Number of processing threads.", t => this.NumThreads = int.Parse(t));

            OptionSet.Add(
                "num-fetch-threads:", "Number of fetcher threads.", t => this.NumConsumerFetchers = int.Parse(t));

            this.ParseArguments();
            this.EnsureMinimalParameters();

            this.ConsumerConfig = new ConsumerConfig
                                      {
                                          GroupId = this.GroupId,
                                          SocketReceiveBufferBytes = this.SocketReceiveBufferBytes,
                                          FetchMessageMaxBytes = this.FetchMessageMaxBytes,
                                          AutoOffsetReset = this.AutoOffsetReset ? "largest" : "smallest",
                                          ZooKeeper = new ZkConfig { ZkConnect = this.ZookeeperConnect },
                                          ConsumerTimeoutMs = 5000,
                                          NumConsumerFetchers = this.NumConsumerFetchers
                                      };
        }

        private void EnsureMinimalParameters()
        {
            if (string.IsNullOrEmpty(this.ZookeeperConnect))
            {
                Console.WriteLine("Missing required argument: zookeeper");
                this.PrintUsage();
                Environment.Exit(1);
            }

            if (string.IsNullOrEmpty(this.Topic))
            {
                Console.WriteLine("Missing required argument: topic");
                this.PrintUsage();
                Environment.Exit(1);
            }
        }
    }

    internal class ConsumerPerfThread
    {
        private int threadId;

        private string name;

        private KafkaStream<byte[], byte[]> stream;

        private ConsumerPerfConfig config;

        private AtomicLong totalMessagesRead;

        private AtomicLong totalBytesRead;

        public ConsumerPerfThread(int threadId, string name, KafkaStream<byte[], byte[]> stream, ConsumerPerfConfig config, AtomicLong totalMessagesRead, AtomicLong totalBytesRead)
        {
            this.threadId = threadId;
            this.name = name;
            this.stream = stream;
            this.config = config;
            this.totalMessagesRead = totalMessagesRead;
            this.totalBytesRead = totalBytesRead;
        }

        private readonly CountdownEvent shutdownLatch = new CountdownEvent(1);

        public void Shutdown()
        {
            this.shutdownLatch.Wait();
        }

        public void Run()
        {
            var bytesRead = 0L;
            var messagesRead = 0L;
            var start = DateTime.Now;
            var lastReportTime = start;
            var lastBytesRead = 0L;
            var lastMessagesRead = 0L;

            try
            {
                var iter = this.stream.GetEnumerator();
                while (messagesRead < this.config.NumMessages)
                {
                    iter.MoveNext();
                    var messageAndMetadata = iter.Current;
                    messagesRead++;
                    bytesRead += messageAndMetadata.Message.Length;

                    if (messagesRead % this.config.ReportingInterval == 0)
                    {
                        if (this.config.ShowDetailedStats)
                        {
                            this.PrintMessage(
                                this.threadId,
                                bytesRead,
                                lastBytesRead,
                                messagesRead,
                                lastMessagesRead,
                                lastReportTime,
                                DateTime.Now);
                        }

                        lastReportTime = DateTime.Now;
                        lastMessagesRead = messagesRead;
                        lastBytesRead = bytesRead;
                    }
                }
            }
            catch (ThreadInterruptedException)
            {
            }
            catch (ConsumerTimeoutException)
            {
            }

            this.totalMessagesRead.AddAndGet(messagesRead);
            this.totalBytesRead.AddAndGet(bytesRead);
            if (this.config.ShowDetailedStats)
            {
                this.PrintMessage(this.threadId, bytesRead, lastBytesRead, messagesRead, lastMessagesRead, start, DateTime.Now);
            }

            this.ShutdownComplete();
        }

        private void PrintMessage(
            int id,
            long bytesRead,
            long lastBytesRead,
            long messagesRead,
            long lastMessagesRead,
            DateTime start,
            DateTime end)
        {
            var elapsedMs = (end - start).TotalMilliseconds;
            var totalMBRead = (bytesRead * 1.0) / (1024 * 1024);
            var mbRead = ((bytesRead - lastBytesRead) * 1.0) / (1024 * 1024);
            Console.WriteLine(
                "{0}, {1}, {2}, {3:f2}, {4:f2}, {5}, {6:f2}", 
                end.ToString(this.config.DateFormat),
                id,
                this.config.ConsumerConfig.FetchMessageMaxBytes,
                totalMBRead,
                1000.0 * (mbRead / elapsedMs),
                messagesRead,
                ((messagesRead - lastMessagesRead) / elapsedMs) * 1000.0);
        }

        private void ShutdownComplete()
        {
            this.shutdownLatch.Signal();
        }
    }
}
