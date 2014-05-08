namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;

    using Kafka.Client.Client;
    using Kafka.Client.Clusters;
    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Extensions;
    using Kafka.Client.Server;
    using Kafka.Client.Utils;
    using Kafka.Client.ZKClient;

    using Spring.Threading.Locks;

    /// <summary>
    /// Usage:
    /// Once ConsumerFetcherManager is created, StartConnections() and StopAllConnections() can be called repeatedly
    /// until Shutdown() is called.
    /// </summary>
    internal class ConsumerFetcherManager : AbstractFetcherManager
    {
        private readonly string consumerIdString;

        private readonly ConsumerConfig config;

        private readonly ZkClient zkClient;

        public ConsumerFetcherManager(string consumerIdString, ConsumerConfig config, ZkClient zkClient)
            : base(
                string.Format("ConsumerFetcherManager-{0}", DateTimeHelper.CurrentTimeMilis()), config.ClientId, config.NumConsumerFetchers)
        {
            this.NoLeaderPartitionSet = new HashSet<TopicAndPartition>();
            this.consumerIdString = consumerIdString;
            this.config = config;
            this.zkClient = zkClient;

            this.@lock = new ReentrantLock();
            this.cond = this.@lock.NewCondition();
        }

        private IDictionary<TopicAndPartition, PartitionTopicInfo> partitionMap;

        private Cluster cluster;

        private HashSet<TopicAndPartition> NoLeaderPartitionSet { get; set; }

        private ReentrantLock @lock;

        private ICondition cond;

        private ShutdownableThread leaderFinderThread;

        private AtomicInteger correlationId = new AtomicInteger(0);

        public override AbstractFetcherThread CreateFetcherThread(int fetcherId, Broker sourceBroker)
        {
            return
                new ConsumerFetcherThread(
                    string.Format("ConsumerFetcherThread-{0}-{1}-{2}", this.consumerIdString, fetcherId, sourceBroker.Id),
                    this.config,
                    sourceBroker,
                    this.partitionMap,
                    this);
        }

        public void StartConnections(List<PartitionTopicInfo> topicInfos, Cluster cluster)
        {
            this.leaderFinderThread = new LeaderFinderThread(this, this.consumerIdString + "-leader-finder-thread");
            this.leaderFinderThread.Start();

            this.@lock.Lock();
            try
            {
                this.partitionMap = topicInfos.ToDictionary(x => new TopicAndPartition(x.Topic, x.PartitionId), x => x);
                this.cluster = cluster;
                var noLeaders = topicInfos.Select(x => new TopicAndPartition(x.Topic, x.PartitionId)).ToList();
                foreach (var noLeader in noLeaders)
                {
                    this.NoLeaderPartitionSet.Add(noLeader);
                }

                this.cond.SignalAll();
            }
            finally
            {
                this.@lock.Unlock();
            }
        }

        public void StopConnections()
        {
            /*
            * Stop the leader finder thread first before stopping fetchers. Otherwise, if there are more partitions without
            * leader, then the leader finder thread will process these partitions (before shutting down) and add fetchers for
            * these partitions.
            */
            Logger.InfoFormat("Stopping leader finder thread");
            if (this.leaderFinderThread != null)
            {
                this.leaderFinderThread.Shutdown();
                this.leaderFinderThread = null;
            }

            Logger.InfoFormat("Stopping all fetchers");
            this.CloseAllFetchers();

            // no need to hold the lock for the following since leaderFindThread and all fetchers have been stopped
            this.partitionMap = null;
            this.NoLeaderPartitionSet.Clear();

            Logger.InfoFormat("All connections stopped");
        }

        public void AddPartitionsWithError(IEnumerable<TopicAndPartition> partitionList)
        {
            Logger.DebugFormat("Adding partitions with error {0}", string.Join(",", partitionList));
            this.@lock.Lock();
            try
            {
                if (this.partitionMap != null)
                {
                    foreach (var partiton in partitionList)
                    {
                        this.NoLeaderPartitionSet.Add(partiton);
                    }

                    this.cond.SignalAll();
                }
            }
            finally
            {
                this.@lock.Unlock();
            }
        }

        public class LeaderFinderThread : ShutdownableThread
        {
            private ConsumerFetcherManager parent;

            public LeaderFinderThread(ConsumerFetcherManager parent, string name)
                : base(name)
            {
                this.parent = parent;
            }

            public override void DoWork()
            {
                var leaderForPartitionsMap = new Dictionary<TopicAndPartition, Broker>();
                this.parent.@lock.Lock();
                try
                {
                    while (this.parent.NoLeaderPartitionSet.Count == 0)
                    {
                        Logger.Debug("No partition for leader election.");
                        this.parent.cond.Await();
                    }

                    Logger.DebugFormat("Partitions without leader {0}", string.Join(",", this.parent.NoLeaderPartitionSet));
                    var brokers = ZkUtils.GetAllBrokersInCluster(this.parent.zkClient);
                    var topicsMetadata =
                        ClientUtils.FetchTopicMetadata(
                            new HashSet<string>(this.parent.NoLeaderPartitionSet.Select(m => m.Topic)),
                            brokers,
                            this.parent.config.ClientId,
                            this.parent.config.SocketTimeoutMs,
                            this.parent.correlationId.GetAndIncrement()).TopicsMetadata;

                    if (Logger.IsDebugEnabled)
                    {
                        foreach (var topicMetadata in topicsMetadata)
                        {
                            Logger.Debug(topicMetadata);
                        }
                    }

                    foreach (var tmd in topicsMetadata)
                    {
                        var topic = tmd.Topic;
                        foreach (var pmd in tmd.PartitionsMetadata)
                        {
                            var topicAndPartition = new TopicAndPartition(topic, pmd.PartitionId);
                            if (pmd.Leader != null && this.parent.NoLeaderPartitionSet.Contains(topicAndPartition))
                            {
                                var leaderBroker = pmd.Leader;
                                leaderForPartitionsMap[topicAndPartition] = leaderBroker;
                                this.parent.NoLeaderPartitionSet.Remove(topicAndPartition);
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    if (!isRunning.Get())
                    {
                        throw; /* If this thread is stopped, propagate this exception to kill the thread. */
                    }
                    else
                    {
                        Logger.Warn("Failed to find leader for " + string.Join(",", this.parent.NoLeaderPartitionSet), e);
                    }
                }
                finally
                {
                    this.parent.@lock.Unlock();
                }

                try
                {
                    this.parent.AddFetcherForPartitions(
                        leaderForPartitionsMap.ToDictionary(
                            kvp => kvp.Key,
                            kvp =>
                            new BrokerAndInitialOffset(kvp.Value, this.parent.partitionMap.Get(kvp.Key).GetFetchOffset())));
                }
                catch (Exception e)
                {
                    if (!isRunning.Get())
                    {
                        throw; /* If this thread is stopped, propagate this exception to kill the thread. */
                    }
                    else
                    {
                        Logger.Warn(string.Format("Failed to add leader for partitions {0}; will retry", string.Join(",", leaderForPartitionsMap.Keys)), e);
                        this.parent.@lock.Lock();

                        foreach (var leader in leaderForPartitionsMap.Keys)
                        {
                            this.parent.NoLeaderPartitionSet.Add(leader);
                        }

                        this.parent.@lock.Unlock();
                    }
                }

                this.parent.ShutdownIdleFetcherThreads();
                Thread.Sleep(this.parent.config.RefreshLeaderBackoffMs);
            }
        }
    }
}