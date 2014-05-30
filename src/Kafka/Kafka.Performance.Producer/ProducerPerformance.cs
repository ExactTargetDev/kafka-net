namespace Kafka.Performance.Producer
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using System.Threading;

    using Kafka.Client.Cfg;
    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Messages;
    using Kafka.Client.Metrics;
    using Kafka.Client.Producers;
    using Kafka.Client.Serializers;
    using Kafka.Performance.Common;

    using log4net;

    public class ProducerPerformance
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public static void Main(string[] args)
        {
            var config = new ProducerPerfConfig(args);
            if (!config.IsFixedSize)
            {
                Logger.Warn("Throughput will be slower due to changing message size per request");
            }

            var totalBytesSent = new AtomicLong(0);
            var totalMessagesSent = new AtomicLong(0);

            var allDone = new CountdownEvent(config.NumThreads);

            var start = DateTime.Now;
            var rand = new Random();

            if (!config.HideHeader)
            {
                Console.WriteLine("start.time, end.time, compression, message.size, batch.size, total.data.sent.in.MB, MB.sec, " +
                        "total.data.sent.in.nMsg, nMsg.sec");
            }

            for (var i = 0; i < config.NumThreads; i++)
            {
                new Thread(() => new ProducerThread(i, config, totalBytesSent, totalMessagesSent, allDone, rand).Run()).Start();
            }

            allDone.Wait();
            var end = DateTime.Now;
            var elapsedSecs = (end - start).TotalSeconds;
            var totalMBSent = (totalBytesSent.Get() * 1.0) / (1024 * 1024);
            Console.WriteLine(
                "{0}; {1}; {2}; {3}; {4}; {5:f2}; {6:f4}; {7}; {8:f4}",
                start.ToString(config.DateFormat),
                end.ToString(config.DateFormat),
                config.CompressionCodec,
                config.MessageSize,
                config.BatchSize,
                totalMBSent,
                totalMBSent / elapsedSecs,
                totalMessagesSent.Get(),
                totalMessagesSent.Get() / elapsedSecs);
        } 
    }

    internal class ProducerPerfConfig : PerfConfig
    {
        public string TopicsStr { get; set; }

        public string[] Topics 
        { 
            get
            {
                return this.TopicsStr.Split(',');
            } 
        } 

        public string BrokerList { get; set; }

        public bool IsFixedSize { get; set; }

        public bool IsSync { get; set; }

        public int NumThreads { get; set; }

        public CompressionCodecs CompressionCodec
        {
            get
            {
                return Client.Messages.CompressionCodec.GetCompressionCodec(this.CompressionCodecInt);
            }
        }

        public bool SeqIdMode { get; set; }

        public int InitialMessageId { get; set; }

        public int ProducerRequestTimeoutMs { get; set; }
        
        public short ProducerRequestRequiredAcks { get; set; }

        public int ProducerNumRetries { get; set; }

        public int ProducerRetryBackoffMs { get; set; }

        public bool CsvMetricsReporterEnabled { get; set; }

        public int MessageSendGapMs { get; set; }

        public string MetricsDirectory { get; set; }

        public ProducerPerfConfig(string[] args)
        {
            this.Args = args;

            this.ProducerRequestTimeoutMs = 3000;
            this.ProducerNumRetries = 3;
            this.ProducerRetryBackoffMs = 100;
            this.ProducerRequestRequiredAcks = -1;
            this.NumThreads = 1;
            this.MessageSendGapMs = 0;
            this.IsFixedSize = true;

            OptionSet.Add(
                "broker-list=",
                "REQUIRED: broker info (the list of broker host and port for bootstrap.",
                b => this.BrokerList = b);

            OptionSet.Add(
                "topics=", "REQUIRED: The comma separated list of topics to produce to", t => this.TopicsStr = t);

            OptionSet.Add(
                "request-timeout-ms:",
                "The produce request timeout in ms",
                t => this.ProducerRequestTimeoutMs = int.Parse(t));

            OptionSet.Add(
                "producer-num-retries:", "The producer retries number", x => this.ProducerNumRetries = int.Parse(x));

            OptionSet.Add(
                "producer-retry-backoff-ms:",
                "The producer retry backoff time in milliseconds",
                x => this.ProducerRetryBackoffMs = int.Parse(x));

            OptionSet.Add(
                "request-num-acks:",
                "Number of acks required for producer request to complete",
                x => this.ProducerRequestRequiredAcks = short.Parse(x));

            OptionSet.Add(
                "vary-message-size",
                "If set, message size will vary up to the given maximum.",
                x => this.IsFixedSize = false);

            OptionSet.Add("sync", "If set, messages are sent synchronously.", x => this.IsSync = true);

            OptionSet.Add("threads:", "Number of sending threads.", x => this.NumThreads = int.Parse(x));

            OptionSet.Add(
                "initial-message-id:",
                "The is used for generating test data, If set, messages will be tagged with an ID and sent by producer starting from this ID sequentially. Message content will be String type and in the form of 'Message:000...1:xxx...'",
                x => this.InitialMessageId = int.Parse(x));

            OptionSet.Add(
                "message-send-gap-ms:",
                "If set, the send thread will wait for specified time between two sends",
                x => this.MessageSendGapMs = int.Parse(x));

            OptionSet.Add(
                "csv-reporter-enabled",
                "If set, the CSV metrics reporter will be enabled",
                x => this.CsvMetricsReporterEnabled = true);

            OptionSet.Add(
                "metrics-dir:",
                "If csv-reporter-enable is set, and this parameter is" + "set, the csv metrics will be outputed here",
                x => this.MetricsDirectory = x);

            this.ParseArguments();
            this.EnsureMinimalParameters();

            if (this.CsvMetricsReporterEnabled)
            {
                var props = new Dictionary<string, string>();
                props["kafka.metrics.polling.interval.secs"] = "1";
                props["kafka.metrics.reporters"] = "kafka.metrics.KafkaCSVMetricsReporter";
                if (string.IsNullOrEmpty(this.MetricsDirectory))
                {
                    props["kafka.csv.metrics.dir"] = this.MetricsDirectory;
                }
                else
                {
                    props["kafka.csv.metrics.dir"] = "kafka_metrics";
                }

                props["kafka.csv.metrics.reporter.enabled"] = "true";

                KafkaMetricsReporter.StartReporters(new Config());  //TODO: change me!
            }
        }

        private void EnsureMinimalParameters()
        {
            if (this.NumMessages == long.MaxValue)
            {
                Console.WriteLine("Missing required argument: messages");
                this.PrintUsage();
                Environment.Exit(1);
            }

            if (string.IsNullOrEmpty(this.BrokerList))
            {
                Console.WriteLine("Missing required argument: broker-list");
                this.PrintUsage();
                Environment.Exit(1);
            }

            if (string.IsNullOrEmpty(this.TopicsStr))
            {
                Console.WriteLine("Missing required argument: topics");
                this.PrintUsage();
                Environment.Exit(1);
            }
        }
    }

    internal class ProducerThread
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly int threadId;

        private ProducerPerfConfig config;

        private AtomicLong totalBytesSent;

        private AtomicLong totalMessagesSent;

        private CountdownEvent allDone;

        private Random rand;

        // generate the sequential message ID
        private string SEP = ":"; // message field separator

        private string messageIdLabel = "MessageID";

        private string threadIdLabel = "ThreadID";

        private string topicLabel = "Topic";

        private string leftPaddedSeqId = "";

        private ProducerConfig producerConfig;

        private Producer<long, byte[]> producer;

        private int seqIdNumDigit = 10; // no. of digits for max int value

        private long messagesPerThread;

        public ProducerThread(int threadId, ProducerPerfConfig config, AtomicLong totalBytesSent, AtomicLong totalMessagesSent, CountdownEvent allDone, Random rand)
        {
            this.threadId = threadId;
            this.config = config;
            this.totalBytesSent = totalBytesSent;
            this.totalMessagesSent = totalMessagesSent;
            this.allDone = allDone;
            this.rand = rand;

            this.producerConfig = new ProducerConfig();

            this.producerConfig.Brokers =
                config.BrokerList.Split(',')
                      .Select(
                          (s, idx) =>
                          new BrokerConfiguration
                              {
                                  BrokerId = idx,
                                  Host = s.Split(':')[0],
                                  Port = int.Parse(s.Split(':')[1])
                              }).ToList();

            this.producerConfig.CompressionCodec = config.CompressionCodec;
            
            this.producerConfig.SendBufferBytes = 64 * 1024;
            if (!config.IsSync)
            {
                this.producerConfig.ProducerType = ProducerTypes.Async;
                this.producerConfig.BatchNumMessages = config.BatchSize;
                this.producerConfig.QueueEnqueueTimeoutMs = -1;
            }
            
            this.producerConfig.ClientId = "ProducerPerformance";
            this.producerConfig.RequestRequiredAcks = config.ProducerRequestRequiredAcks;
            this.producerConfig.RequestTimeoutMs = config.ProducerRequestTimeoutMs;
            this.producerConfig.MessageSendMaxRetries = config.ProducerNumRetries;
            this.producerConfig.RetryBackoffMs = config.ProducerRetryBackoffMs;
            
            this.producerConfig.Serializer = typeof(DefaultEncoder).AssemblyQualifiedName;
            this.producerConfig.KeySerializer = typeof(NullEncoder<long>).AssemblyQualifiedName;
            
            this.producer = new Producer<long, byte[]>(this.producerConfig);

            this.messagesPerThread = config.NumMessages / config.NumThreads;
            Logger.DebugFormat("Messages per thread = {0}", this.messagesPerThread);
        }

        private byte[] GenerateMessageWithSeqId(string topic, long msgId, int msgSize)
        {
            // Each thread gets a unique range of sequential no. for its ids.
            // Eg. 1000 msg in 10 threads => 100 msg per thread
            // thread 0 IDs :   0 ~  99
            // thread 1 IDs : 100 ~ 199
            // thread 2 IDs : 200 ~ 299
            // . . .
            this.leftPaddedSeqId = string.Format("{0:D10}", msgId);

            var msgHeader = this.topicLabel + this.SEP + topic + this.SEP + this.threadIdLabel + this.SEP + this.threadId + this.SEP + this.messageIdLabel
                            + this.SEP + this.leftPaddedSeqId;

            var seqMsgString = msgHeader.PadLeft(msgSize, 'x');
            return Encoding.UTF8.GetBytes(seqMsgString);
        }

        private Tuple<KeyedMessage<long, byte[]>, int> GenerateProducerData(string topic, long messageId)
        {
            var msgSize = this.config.IsFixedSize ? this.config.MessageSize : 1 + this.rand.Next(this.config.MessageSize);
            var message = this.config.SeqIdMode
                              ? this.GenerateMessageWithSeqId(
                                  topic, this.config.InitialMessageId + (this.messagesPerThread * this.threadId) + messageId, msgSize)
                              : new byte[msgSize];
            return Tuple.Create(new KeyedMessage<long, byte[]>(topic, messageId, message), message.Length);
        }

        public void Run()
        {
            var bytesSent = 0L;
            var nSends = 0;
            var j = 0L;
            while (j < this.messagesPerThread)
            {
                try
                {
                    foreach (var topic in this.config.Topics)
                    {
                        var producerDataAndBytes = this.GenerateProducerData(topic, j);
                        bytesSent += producerDataAndBytes.Item2;
                        this.producer.Send(producerDataAndBytes.Item1);
                        nSends++;
                        if (this.config.MessageSendGapMs > 0)
                        {
                            Thread.Sleep(this.config.MessageSendGapMs);
                        }
                    }
                }
                catch (Exception e)
                {
                    Logger.Error("Error sending messages", e);
                }

                j++;
            }

            this.producer.Dispose();
            this.totalBytesSent.AddAndGet(bytesSent);
            this.totalMessagesSent.AddAndGet(nSends);
            this.allDone.Signal();
        }
    }
}