namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Generic;

    using Kafka.Client.Cfg;
    using Kafka.Client.Clusters;
    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Locks;
    using Kafka.Client.Server;
    using Kafka.Client.Utils;
    using Kafka.Client.ZKClient;

    using System.Linq;

    public class ConsumerFetcherManager : AbstractFetcherManager
    {
        private readonly string consumerIdString;

        private readonly ConsumerConfiguration config;

        private readonly ZkClient zkClient;

        public ConsumerFetcherManager(string consumerIdString, ConsumerConfiguration config, ZkClient zkClient)
            : base(
                string.Format("ConsumerFetcherManager-{0}", DateTime.Now), config.ClientId, config.NumConsumerFetchers)

        {
            this.consumerIdString = consumerIdString;
            this.config = config;
            this.zkClient = zkClient;

            this.@lock = new ReentrantLock();
            this.cond = this.@lock.NewCondition();

        }

        private IDictionary<TopicAndPartition, PartitionTopicInfo> partitionMap;

        private Cluster cluster;

        private HashSet<TopicAndPartition> noLeaderPartitionSet = new HashSet<TopicAndPartition>();

        private ReentrantLock @lock;

        private ICondition cond;

        private ShutdownableThread leaderFinderThread;

        private AtomicInteger correlationId = new AtomicInteger(0);

        //TODO: LeaderFinderThread

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
            leaderFinderThread = new LeaderFinderThread(consumerIdString + "-leader-finder-thread");
            leaderFinderThread.Start();

            @lock.Lock();
            try
            {

                partitionMap = topicInfos.ToDictionary(x => new TopicAndPartition(x.Topic, x.PartitionId), x => x);
                this.cluster = cluster;
                var noLeaders = topicInfos.Select(x => new TopicAndPartition(x.Topic, x.PartitionId)).ToList();
                foreach (var noLeader in noLeaders)
                {
                    noLeaderPartitionSet.Add(noLeader);
                }
                cond.SignalAll();
            }
            finally
            {
                @lock.Unlock();
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
            noLeaderPartitionSet.Clear();

            Logger.InfoFormat("All connections stopped");
        }

        public void AddPartitionsWithError(IList<TopicAndPartition> partitionList)
        {
            Logger.DebugFormat("Adding partitions with error {0}", string.Join(",", partitionList));
            lock (this.@lock)
            {
                if (this.partitionMap != null)
                {
                    foreach (var partiton in partitionList)
                    {
                        this.noLeaderPartitionSet.Add(partiton);
                    }
                    this.cond.SignalAll();
                }
            }
        }

    }

    public class LeaderFinderThread : ShutdownableThread
    {
        public LeaderFinderThread(string name)
            : base(name)
        {
        }

        public override void DoWork()
        {
            throw new NotImplementedException();
            /*val leaderForPartitionsMap = new HashMap[TopicAndPartition, Broker]
      lock.lock()
      try {
        while (noLeaderPartitionSet.isEmpty) {
          trace("No partition for leader election.")
          cond.await()
        }

        trace("Partitions without leader %s".format(noLeaderPartitionSet))
        val brokers = getAllBrokersInCluster(zkClient)
        val topicsMetadata = ClientUtils.fetchTopicMetadata(noLeaderPartitionSet.map(m => m.topic).toSet,
                                                            brokers,
                                                            config.clientId,
                                                            config.socketTimeoutMs,
                                                            correlationId.getAndIncrement).topicsMetadata
        if(logger.isDebugEnabled) topicsMetadata.foreach(topicMetadata => debug(topicMetadata.toString()))
        topicsMetadata.foreach { tmd =>
          val topic = tmd.topic
          tmd.partitionsMetadata.foreach { pmd =>
            val topicAndPartition = TopicAndPartition(topic, pmd.partitionId)
            if(pmd.leader.isDefined && noLeaderPartitionSet.contains(topicAndPartition)) {
              val leaderBroker = pmd.leader.get
              leaderForPartitionsMap.put(topicAndPartition, leaderBroker)
              noLeaderPartitionSet -= topicAndPartition
            }
          }
        }
      } catch {
        case t: Throwable => {
            if (!isRunning.get())
              throw t /* If this thread is stopped, propagate this exception to kill the thread. *
            else
              warn("Failed to find leader for %s".format(noLeaderPartitionSet), t)
          }
      } finally {
        lock.unlock()
      }

      try {
        addFetcherForPartitions(leaderForPartitionsMap.map{
          case (topicAndPartition, broker) =>
            topicAndPartition -> BrokerAndInitialOffset(broker, partitionMap(topicAndPartition).getFetchOffset())}
        )
      } catch {
        case t: Throwable => {
          if (!isRunning.get())
            throw t /* If this thread is stopped, propagate this exception to kill the thread. *
          else {
            warn("Failed to add leader for partitions %s; will retry".format(leaderForPartitionsMap.keySet.mkString(",")), t)
            lock.lock()
            noLeaderPartitionSet ++= leaderForPartitionsMap.keySet
            lock.unlock()
          }
        }
      }

      shutdownIdleFetcherThreads()
      Thread.sleep(config.refreshLeaderBackoffMs)*/
        }
    }

}