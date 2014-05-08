namespace Kafka.Client.Server
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;

    using Kafka.Client.Clusters;
    using Kafka.Client.Common;
    using Kafka.Client.Extensions;

    using log4net;

    /// <summary>
    /// 
    /// Note: original namespace: kafka.server
    /// </summary>
    internal abstract class AbstractFetcherManager
    {
        protected static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        protected string Name { get; set; }

        public string MetricPrefix { get; protected set; }

        public int NumFetchers { get; protected set; }

        internal AbstractFetcherManager(string name, string metricPrefix, int numFetchers = 1)
        {
            this.Name = name;
            this.MetricPrefix = metricPrefix;
            this.NumFetchers = numFetchers;
        }

        private readonly Dictionary<BrokerAndFetcherId, AbstractFetcherThread> fetcherThreadMap = new Dictionary<BrokerAndFetcherId, AbstractFetcherThread>();

        private readonly object mapLock = new object();

        private int GetFetcherId(string topic, int partitionId)
        {
            return Math.Abs((31 * topic.GetHashCode()) + partitionId) % this.NumFetchers;
        }

        // to be defined in subclass to create a specific fetcher
        public abstract AbstractFetcherThread CreateFetcherThread(int fetcherId, Broker sourceBroker);

        public void AddFetcherForPartitions(IDictionary<TopicAndPartition, BrokerAndInitialOffset> partitionAndOffsets)
        {
            lock (this.mapLock)
            {
                var partitionsPerFetcher = partitionAndOffsets.GroupByScala(
                    kvp =>
                        { 
                            var topicAndPartition = kvp.Key;
                            var brokerAndInitialOffset = kvp.Value;
                            return new BrokerAndFetcherId(
                                brokerAndInitialOffset.Broker,
                                this.GetFetcherId(topicAndPartition.Topic, topicAndPartition.Partiton));
                        });
                foreach (var kvp in partitionsPerFetcher)
                {
                    var brokerAndFetcherId = kvp.Key;
                    var partitionAndOffset = kvp.Value;

                    AbstractFetcherThread fetcherThread;
                    if (this.fetcherThreadMap.TryGetValue(brokerAndFetcherId, out fetcherThread) == false)
                    {
                        fetcherThread = this.CreateFetcherThread(
                            brokerAndFetcherId.FetcherId, brokerAndFetcherId.Broker);
                        this.fetcherThreadMap[brokerAndFetcherId] = fetcherThread;
                        fetcherThread.Start();
                    }

                    this.fetcherThreadMap.Get(brokerAndFetcherId)
                                    .AddPartitions(
                                        partitionAndOffsets.ToDictionary(x => x.Key, x => x.Value.InitOffset));
                }
            }

            Logger.InfoFormat(
                "Added fetcher for partitons {0}",
                string.Join(
                    ", ",
                    partitionAndOffsets.Select(
                        kvp =>
                            {
                                var topicAndPartition = kvp.Key;
                                var brokerAndInitialOffset = kvp.Value;
                                return "[" + topicAndPartition + ", initOffset " + brokerAndInitialOffset.InitOffset + " to broker "
                                       + brokerAndInitialOffset.Broker + "]";
                }).ToArray()));
        }

        public void RemoveFetcherForPartitions(HashSet<TopicAndPartition> partitions)
        {
            lock (this.mapLock)
            {
                foreach (var keyAndFetcher in this.fetcherThreadMap)
                {
                    keyAndFetcher.Value.RemovePartitions(partitions);
                }
            }

            Logger.InfoFormat("Removed fetcher for partitions {0}", string.Join(",", partitions));
        }

        public void ShutdownIdleFetcherThreads()
        {
            lock (this.mapLock)
            {
                var keysToBeRemoted = new HashSet<BrokerAndFetcherId>();
                foreach (var keyAndFetcher in this.fetcherThreadMap)
                {
                    var key = keyAndFetcher.Key;
                    var fetcher = keyAndFetcher.Value;
                    if (fetcher.PartitionCount() <= 0)
                    {
                        fetcher.Shutdown();
                        keysToBeRemoted.Add(key);
                    }
                }

                foreach (var key in keysToBeRemoted)
                {
                    this.fetcherThreadMap.Remove(key);
                }
            }
        }

        public void CloseAllFetchers()
        {
            lock (this.mapLock)
            {
                foreach (var fetcher in this.fetcherThreadMap.Values)
                {
                    fetcher.Shutdown();
                }

                this.fetcherThreadMap.Clear();
            }
        }
    }

    public class BrokerAndFetcherId
    {
        public Broker Broker { get; private set; }

        public int FetcherId { get; private set; }

        public BrokerAndFetcherId(Broker broker, int fetcherId)
        {
            this.Broker = broker;
            this.FetcherId = fetcherId;
        }

        protected bool Equals(BrokerAndFetcherId other)
        {
            return Equals(this.Broker, other.Broker) && this.FetcherId == other.FetcherId;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj.GetType() != this.GetType())
            {
                return false;
            }

            return this.Equals((BrokerAndFetcherId)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((this.Broker != null ? this.Broker.GetHashCode() : 0) * 397) ^ this.FetcherId;
            }
        }
    }

    public class BrokerAndInitialOffset
    {
        public Broker Broker { get; private set; }

        public long InitOffset { get; private set; }

        public BrokerAndInitialOffset(Broker broker, long initOffset)
        {
            this.Broker = broker;
            this.InitOffset = initOffset;
        }

        protected bool Equals(BrokerAndInitialOffset other)
        {
            return Equals(this.Broker, other.Broker) && this.InitOffset == other.InitOffset;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj.GetType() != this.GetType())
            {
                return false;
            }

            return this.Equals((BrokerAndInitialOffset)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((this.Broker != null ? this.Broker.GetHashCode() : 0) * 397) ^ this.InitOffset.GetHashCode();
            }
        }
    }
}