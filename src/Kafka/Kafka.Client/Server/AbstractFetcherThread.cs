using Kafka.Client.Cfg;

namespace Kafka.Client.Server
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using Kafka.Client.Api;
    using Kafka.Client.Clusters;
    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Consumers;
    using Kafka.Client.Extensions;
    using Kafka.Client.Messages;
    using Kafka.Client.Utils;

    using Spring.Threading.Locks;

    internal abstract class AbstractFetcherThread : ShutdownableThread
    {
        private string clientId;

        private Broker sourceBroker;

        private int socketTimeout;

        private int socketBufferSize;

        private int fetchSize;

        private int fetcherBrokerId;

        private int maxWait;

        private int minBytes;

        private readonly IDictionary<TopicAndPartition, long> partitionMap = new Dictionary<TopicAndPartition, long>();

        private readonly ReentrantLock partitionMapLock;

        private readonly ICondition partitionMapCond;

        protected readonly SimpleConsumer simpleConsumer;

        private readonly string brokerInfo;

        private readonly ClientIdAndBroker metricId;

        public FetcherStats FetcherStats { get; private set; }

        public FetcherLagStats FetcherLagStats { get; private set; }

        private readonly FetchRequestBuilder fetchRequestBuilder;

        internal AbstractFetcherThread(
            string name,
            string clientId,
            Broker sourceBroker,
            int socketTimeout,
            int socketBufferSize,
            int fetchSize,
            int fetcherBrokerId = -1,
            int maxWait = 0,
            int minBytes = 1,
            bool isInterruptible = true)
            : base(name, isInterruptible)
        {
            this.clientId = clientId;
            this.sourceBroker = sourceBroker;
            this.socketTimeout = socketTimeout;
            this.socketBufferSize = socketBufferSize;
            this.fetchSize = fetchSize;
            this.fetcherBrokerId = fetcherBrokerId;
            this.maxWait = maxWait;
            this.minBytes = minBytes;

            this.partitionMapLock = new ReentrantLock();
            this.partitionMapCond = this.partitionMapLock.NewCondition();
            this.simpleConsumer = new SimpleConsumer(
                sourceBroker.Host, sourceBroker.Port, socketTimeout, socketBufferSize, clientId);
            this.brokerInfo = string.Format("host_{0}-port_{1}", sourceBroker.Host, sourceBroker.Port);

            this.metricId = new ClientIdAndBroker(clientId, this.brokerInfo);

            this.FetcherStats = new FetcherStats(this.metricId);
            this.FetcherLagStats = new FetcherLagStats(this.metricId);
            this.fetchRequestBuilder =
                new FetchRequestBuilder().ClientId(clientId)
                                         .ReplicaId(fetcherBrokerId)
                                         .MaxWait(maxWait)
                                         .MinBytes(minBytes);
        }

        /// <summary>
        ///  process fetched Data
        /// </summary>
        /// <param name="topicAndPartition"></param>
        /// <param name="fetchOffset"></param>
        /// <param name="partitionData"></param>
        public abstract void ProcessPartitionData(
            TopicAndPartition topicAndPartition, long fetchOffset, FetchResponsePartitionData partitionData);

        /// <summary>
        /// handle a partition whose offset is out of range and return a new fetch offset
        /// </summary>
        /// <param name="topicAndPartition"></param>
        /// <returns></returns>
        public abstract long HandleOffsetOutOfRange(TopicAndPartition topicAndPartition);

        /// <summary>
        /// deal with partitions with errors, potentially due to leadership changes
        /// </summary>
        /// <param name="partitions"></param>
        public abstract void HandlePartitionsWithErrors(IEnumerable<TopicAndPartition> partitions);

        public override void Shutdown()
        {
            base.Shutdown();
            this.simpleConsumer.Close();
        }

        public override void DoWork()
        {
            this.partitionMapLock.Lock();
            try
            {
                if (this.partitionMap.Count == 0)
                {
                    this.partitionMapCond.Await(TimeSpan.FromMilliseconds(200));
                }

                foreach (var topicAndOffset in this.partitionMap)
                {
                    var topicAndPartition = topicAndOffset.Key;
                    var offset = topicAndOffset.Value;
                    this.fetchRequestBuilder.AddFetch(topicAndPartition.Topic, topicAndPartition.Partiton, offset, this.fetchSize);
                }
            }
            finally
            {
                this.partitionMapLock.Unlock();
            }

            var fetchRequest = this.fetchRequestBuilder.Build();
            if (fetchRequest.RequestInfo.Count > 0)
            {
                this.ProcessFetchRequest(fetchRequest);
            }
        }

        public void ProcessFetchRequest(FetchRequest fetchRequest)
        {
            var partitionsWithError = new HashSet<TopicAndPartition>();
            FetchResponse response = null;
            try
            {
                Logger.DebugFormat("issuing to broker {0} of fetch request {1}", this.sourceBroker.Id, fetchRequest);
                response = this.simpleConsumer.Fetch(fetchRequest);
            }
            catch (Exception e)
            {
                if (isRunning.Get())
                {
                    Logger.Error("Error in fetch " + fetchRequest, e);
                    this.partitionMapLock.Lock();
                    try
                    {
                        foreach (var key in this.partitionMap.Keys)
                        {
                            partitionsWithError.Add(key);
                        }
                    }
                    finally
                    {
                        this.partitionMapLock.Unlock();
                    }
                }
            }

            this.FetcherStats.RequestRate.Mark();

            if (response != null)
            {
                // process fetched Data
                this.partitionMapLock.Lock();
                try
                {
                    foreach (var topicAndData in response.Data)
                    {
                        var topicAndPartition = topicAndData.Key;
                        var partitionData = topicAndData.Value;
                        var topic = topicAndPartition.Topic;
                        var partitionId = topicAndPartition.Partiton;
                        long currentOffset;
                        if (this.partitionMap.TryGetValue(topicAndPartition, out currentOffset)
                            && fetchRequest.RequestInfo[topicAndPartition].Offset == currentOffset)
                        {
                            // we append to the log if the current offset is defined and it is the same as the offset requested during fetch                     
                            switch (partitionData.Error)
                            {
                                case ErrorMapping.NoError:
                                    try
                                    {
                                        var messages = (ByteBufferMessageSet)partitionData.Messages;
                                        var messageAndOffset =
                                            messages.ShallowIterator().ToEnumerable().LastOrDefault();
                                        var newOffset = messageAndOffset != null
                                                            ? messageAndOffset.NextOffset
                                                            : currentOffset;

                                        this.partitionMap[topicAndPartition] = newOffset;

                                        if (StatSettings.FetcherThreadStatsEnabled)
                                        {
                                            var validBytes = messages.ValidBytes;

                                            this.FetcherLagStats.GetFetcherLagStats(topic, partitionId).Lag = partitionData.Hw
                                                                                                              -
                                                                                                              newOffset;
                                            this.FetcherStats.ByteRate.Mark(validBytes);
                                        }

                                        // Once we hand off the partition Data to the subclass, we can't mess with it any more in this thread
                                        this.ProcessPartitionData(topicAndPartition, currentOffset, partitionData);
                                    }
                                    catch (InvalidMessageException ime)
                                    {
                                        // we log the error and continue. This ensures two things
                                        // 1. If there is a corrupt message in a topic partition, it does not bring the fetcher thread down and cause other topic partition to also lag
                                        // 2. If the message is corrupt due to a transient state in the log (truncation, partial writes can cause this), we simply continue and
                                        //    should get fixed in the subsequent fetches
                                        Logger.ErrorFormat(
                                            "Found invalid messages during fetch for partiton [{0},{1}] offset {2} error {3}",
                                            topic,
                                            partitionId,
                                            currentOffset,
                                            ime.Message);
                                    }
                                    catch (Exception e)
                                    {
                                        throw new KafkaException(
                                            string.Format(
                                                "error processing Data for partition [{0},{1}] offset {2}",
                                                topic,
                                                partitionId,
                                                currentOffset),
                                            e);
                                    }

                                    break;
                                case ErrorMapping.OffsetOutOfRangeCode:
                                    try
                                    {
                                        var newOffset = this.HandleOffsetOutOfRange(topicAndPartition);
                                        this.partitionMap[topicAndPartition] = newOffset;
                                        Logger.ErrorFormat(
                                            "Current offset {0} for partiton [{1},{2}] out of range; reste offset to {3}",
                                            currentOffset,
                                            topic,
                                            partitionId,
                                            newOffset);
                                    }
                                    catch (Exception e)
                                    {
                                        Logger.Error(
                                            string.Format(
                                                "Error getting offset for partiton [{0},{1}] to broker {2}",
                                                topic,
                                                partitionId,
                                                sourceBroker.Id),
                                            e);
                                        partitionsWithError.Add(topicAndPartition);
                                    }

                                    break;
                                default:
                                    if (isRunning.Get())
                                    {
                                        Logger.ErrorFormat(
                                            "Error for partition [{0},{1}] to broker {2}:{3}",
                                            topic,
                                            partitionId,
                                            this.sourceBroker.Id,
                                            ErrorMapping.ExceptionFor(partitionData.Error).GetType().Name);
                                        partitionsWithError.Add(topicAndPartition);
                                    }

                                    break;
                            }
                        }
                    }
                }
                finally
                {
                    this.partitionMapLock.Unlock();
                }
            }

            if (partitionsWithError.Count > 0)
            {
                Logger.DebugFormat("handling partitions with error for {0}", string.Join(",", partitionsWithError));
                this.HandlePartitionsWithErrors(partitionsWithError);
            }
        }

        public void AddPartitions(IDictionary<TopicAndPartition, long> partitionAndOffsets)
        {
            this.partitionMapLock.LockInterruptibly();
            try
            {
                foreach (var topicAndOffset in partitionAndOffsets)
                {
                    var topicAndPartition = topicAndOffset.Key;
                    var offset = topicAndOffset.Value;

                    // If the partitionMap already has the topic/partition, then do not update the map with the old offset
                    if (!this.partitionMap.ContainsKey(topicAndPartition))
                    {
                        this.partitionMap[topicAndPartition] = PartitionTopicInfo.IsOffsetInvalid(offset)
                                                              ? this.HandleOffsetOutOfRange(topicAndPartition)
                                                              : offset;
                    }

                    this.partitionMapCond.SignalAll();
                }
            }
            finally
            {
                this.partitionMapLock.Unlock();
            }
        }

        public void RemovePartitions(ISet<TopicAndPartition> topicAndPartitions)
        {
            this.partitionMapLock.LockInterruptibly();
            try
            {
                foreach (var tp in topicAndPartitions)
                {
                    this.partitionMap.Remove(tp);
                }
            }
            finally
            {
                this.partitionMapLock.Unlock();
            }
        }

        public int PartitionCount()
        {
            this.partitionMapLock.LockInterruptibly();
            try
            {
                return this.partitionMap.Count;
            }
            finally
            {
                this.partitionMapLock.Unlock();
            }
        }
    }

    internal class FetcherLagMetrics
    {
        private readonly AtomicLong lagVal = new AtomicLong(-1);

        public FetcherLagMetrics(ClientIdBrokerTopicPartition metricId)
        {
            MetersFactory.NewGauge(metricId + "-ConsumerLag", () => this.lagVal.Get());
        }

        internal long Lag
        {
            get
            {
                return this.lagVal.Get();
            }

            set
            {
                this.lagVal.Set(value);
            }
        }
    }

    internal class FetcherLagStats
    {
        private readonly ClientIdAndBroker metricId;

        private readonly Func<ClientIdBrokerTopicPartition, FetcherLagMetrics> valueFactory;

        public Pool<ClientIdBrokerTopicPartition, FetcherLagMetrics> Stats { get; private set; }

        public FetcherLagStats(ClientIdAndBroker metricId)
        {
            this.metricId = metricId;
            this.valueFactory = k => new FetcherLagMetrics(k);
            this.Stats = new Pool<ClientIdBrokerTopicPartition, FetcherLagMetrics>(this.valueFactory);
        }

        internal FetcherLagMetrics GetFetcherLagStats(string topic, int partitionId)
        {
            return this.Stats.GetAndMaybePut(
                new ClientIdBrokerTopicPartition(this.metricId.ClientId, this.metricId.BrokerInfo, topic, partitionId));
        }
    }

    internal class FetcherStats
    {
        public FetcherStats(ClientIdAndBroker metricId)
        {
            this.RequestRate = MetersFactory.NewMeter(metricId + "-RequestsPerSec", "requests", TimeSpan.FromSeconds(1));
            this.ByteRate = MetersFactory.NewMeter(metricId + "-BytesPerSec", "bytes", TimeSpan.FromSeconds(1));
        }

        internal IMeter RequestRate { get; private set; }

        internal IMeter ByteRate { get; private set; }
    }

    internal class ClientIdBrokerTopicPartition
    {
        public string ClientId { get; private set; }

        public string BrokerInfo { get; private set; }

        public string Topic { get; private set; }

        public int PartitonId { get; private set; }

        public ClientIdBrokerTopicPartition(string clientId, string brokerInfo, string topic, int partitonId)
        {
            this.ClientId = clientId;
            this.BrokerInfo = brokerInfo;
            this.Topic = topic;
            this.PartitonId = partitonId;
        }
    }
}