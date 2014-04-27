namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    using Kafka.Client.Cfg;
    using Kafka.Client.Client;
    using Kafka.Client.Clusters;
    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Locks;
    using Kafka.Client.Server;
    using Kafka.Client.Utils;
    using Kafka.Client.ZKClient;

    using System.Linq;

    using Kafka.Client.Extensions;

    public class ConsumerFetcherManager : AbstractFetcherManager
    {
        private readonly string consumerIdString;

        private readonly ConsumerConfiguration config;

        private readonly ZkClient zkClient;

        public ConsumerFetcherManager(string consumerIdString, ConsumerConfiguration config, ZkClient zkClient)
            : base(
                string.Format("ConsumerFetcherManager-{0}", DateTime.Now), config.ClientId, config.NumConsumerFetchers)
        {

            this.NoLeaderPartitionSet = new HashSet<TopicAndPartition>();
            this.consumerIdString = consumerIdString;
            this.config = config;
            this.zkClient = zkClient;

            this.Lock = new ReentrantLock();
            this.cond = this.Lock.NewCondition();

        }

        private IDictionary<TopicAndPartition, PartitionTopicInfo> partitionMap;

        private Cluster cluster;

        private HashSet<TopicAndPartition> NoLeaderPartitionSet { get; set; }

        internal ReentrantLock Lock { get; private set; }

        private ICondition cond;

        private ShutdownableThread leaderFinderThread;

        private AtomicInteger correlationId = new AtomicInteger(0);

        public override AbstractFetcherThread CreateFetcherThread(int fetcherId, Broker sourceBroker)
        {
            return
                new ConsumerFetcherThread(
                    string.Format("ConsumerFetcherThread-{0}-{1}-{2}", consumerIdString, fetcherId, sourceBroker.Id),
                    config,
                    sourceBroker,
                    partitionMap,
                    this);
        }

        public void StartConnections(IEnumerable<PartitionTopicInfo> topicInfos, Cluster cluster)
        {
            leaderFinderThread = new LeaderFinderThread(this, consumerIdString + "-leader-finder-thread");
            leaderFinderThread.Start();

            Lock.Lock();
            try
            {

                partitionMap = topicInfos.ToDictionary(x => new TopicAndPartition(x.Topic, x.PartitionId), x => x);
                this.cluster = cluster;
                var noLeaders = topicInfos.Select(x => new TopicAndPartition(x.Topic, x.PartitionId)).ToList();
                foreach (var noLeader in noLeaders)
                {
                    this.NoLeaderPartitionSet.Add(noLeader);
                }
                cond.SignalAll();
            }
            finally
            {
                Lock.Unlock();
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
            if (leaderFinderThread != null)
            {
                leaderFinderThread.Shutdown();
                leaderFinderThread = null;
            }

            Logger.InfoFormat("Stopping all fetchers");
            this.CloseAllFetchers();

            // no need to hold the lock for the following since leaderFindThread and all fetchers have been stopped
            partitionMap = null;
            this.NoLeaderPartitionSet.Clear();

            Logger.InfoFormat("All connections stopped");
        }

        public void AddPartitionsWithError(IEnumerable<TopicAndPartition> partitionList)
        {
            Logger.DebugFormat("Adding partitions with error {0}", string.Join(",", partitionList));
            this.Lock.Lock();
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
                this.Lock.Unlock();
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
                this.parent.Lock.Lock();
                try
                {
                    while (this.parent.NoLeaderPartitionSet.Count == 0)
                    {
                        Logger.Debug("No partition for leader election.");
                        parent.cond.Await();
                    }

                    Logger.DebugFormat("Partitions without leader {0}", string.Join(",", parent.NoLeaderPartitionSet));
                    var brokers = ZkUtils.GetAllBrokersInCluster(parent.zkClient);
                    var topicsMetadata =
                        ClientUtils.FetchTopicMetadata(
                            new HashSet<string>(parent.NoLeaderPartitionSet.Select(m => m.Topic)),
                            brokers,
                            parent.config.ClientId,
                            parent.config.SocketTimeoutMs,
                            parent.correlationId.GetAndIncrement()).TopicsMetadata;

                    if (Logger.IsDebugEnabled)
                    {
                        foreach (var topicMetadata in topicsMetadata)
                        {
                            Logger.Debug(topicMetadata);
                        }
                    }

                    foreach (var tmd in  topicsMetadata)
                    {
                        var topic = tmd.Topic;
                        foreach (var pmd in tmd.PartitionsMetadata)
                        {
                            var topicAndPartition = new TopicAndPartition(topic, pmd.PartitionId);
                            if (pmd.Leader != null && this.parent.NoLeaderPartitionSet.Contains(topicAndPartition))
                            {
                                var leaderBroker = pmd.Leader;
                                leaderForPartitionsMap[topicAndPartition] = leaderBroker;
                                parent.NoLeaderPartitionSet.Remove(topicAndPartition);
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    if (!isRunning.Get())
                    {
                        throw e; /* If this thread is stopped, propagate this exception to kill the thread. */
                    }
                    else
                    {
                        Logger.Warn("Failed to find leader for " + string.Join(",", parent.NoLeaderPartitionSet), e);
                    }
                }
                finally
                {
                    this.parent.Lock.Unlock();
                }

                try
                {
                    parent.AddFetcherForPartitions(
                        leaderForPartitionsMap.ToDictionary(
                            kvp => kvp.Key,
                            kvp =>
                            new BrokerAndInitialOffset(kvp.Value, parent.partitionMap.Get(kvp.Key).GetFetchOffset())));
                }
                catch (Exception e)
                {
                    if (!isRunning.Get())
                    {
                        throw e; /* If this thread is stopped, propagate this exception to kill the thread. */
                    }
                    else
                    {
                        Logger.Warn(string.Format("Failed to add leader for partitions {0}; will retry", string.Join(",", leaderForPartitionsMap.Keys)), e);
                        parent.Lock.Lock();

                        foreach (var leader in leaderForPartitionsMap.Keys)
                        {
                            parent.NoLeaderPartitionSet.Add(leader);
                        }
                        parent.Lock.Unlock();
                    }
                }
               
                parent.ShutdownIdleFetcherThreads();
                Thread.Sleep(parent.config.RefreshLeaderBackoffMs);
            }
        }

    }
}