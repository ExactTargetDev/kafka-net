namespace Kafka.Client.Server
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    using Kafka.Client.Clusters;
    using Kafka.Client.Common;

    using Kafka.Client.Extensions;

    using log4net;

    using System.Linq;

    /// <summary>
    /// 
    /// Note: original namespace: kafka.server
    /// </summary>
    public abstract class AbstractFetcherManager
    {

        protected static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        protected string Name { get; set; }

        public string MetricPrefix { get; protected set; }

        public int NumFetchers { get; protected set; }

        public AbstractFetcherManager(string name, string metricPrefix, int numFetchers = 1)
        {
            this.Name = name;
            this.MetricPrefix = metricPrefix;
            this.NumFetchers = numFetchers;
        }
        private readonly Dictionary<BrokerAndFetcherId, AbstractFetcherThread>  fetcherThreadMap = new Dictionary<BrokerAndFetcherId, AbstractFetcherThread>();

        private readonly object mapLock = new object();

        private int GetFetcherId(string topic, int partitionId)
        {
            return Math.Abs(31 * topic.GetHashCode() + partitionId) % NumFetchers;
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
                    if (fetcherThreadMap.TryGetValue(brokerAndFetcherId, out fetcherThread) == false)
                    {
                        fetcherThread = this.CreateFetcherThread(
                            brokerAndFetcherId.FetcherId, brokerAndFetcherId.Broker);
                        fetcherThreadMap[brokerAndFetcherId] = fetcherThread;
                        fetcherThread.Start();
                    }

                    throw new NotImplementedException();
                    /* TODO
                     * fetcherThreadMap(brokerAndFetcherId).addPartitions(partitionAndOffset.map { case (topicAndPartition, brokerAndInitOffset) =>
          topicAndPartition -> brokerAndInitOffset.initOffset
        })*/
                }
            }

            Logger.InfoFormat("Added fetcher for partitons {0}", string.Join(", ", partitionAndOffsets.Select(kvp =>
                {
                    var topicAndPartition = kvp.Key;
                    var brokerAndInitialOffset = kvp.Value;
                    return "[" + topicAndPartition + ", initOffset " + brokerAndInitialOffset.InitOffset + " to broker "
                           + brokerAndInitialOffset.Broker + "]";
                }).ToArray()));
        }

        /*
     
      
         
         
  def removeFetcherForPartitions(partitions: Set[TopicAndPartition]) {
    mapLock synchronized {
      for ((key, fetcher) <- fetcherThreadMap) {
        fetcher.removePartitions(partitions)
      }
    }
    info("Removed fetcher for partitions %s".format(partitions.mkString(",")))
  }

  def shutdownIdleFetcherThreads() {
    mapLock synchronized {
      val keysToBeRemoved = new mutable.HashSet[BrokerAndFetcherId]
      for ((key, fetcher) <- fetcherThreadMap) {
        if (fetcher.partitionCount <= 0) {
          fetcher.shutdown()
          keysToBeRemoved += key
        }
      }
      fetcherThreadMap --= keysToBeRemoved
    }
  }

  def closeAllFetchers() {
    mapLock synchronized {
      for ( (_, fetcher) <- fetcherThreadMap) {
        fetcher.shutdown()
      }
      fetcherThreadMap.clear()
    }
  }
         */
        //TODO: finish me

    }
}