namespace Kafka.Client.Server
{
    using System;
    using System.Collections.Generic;

    using Kafka.Client.Api;
    using Kafka.Client.Clusters;
    using Kafka.Client.Common;
    using Kafka.Client.Consumers;
    using Kafka.Client.Locks;
    using Kafka.Client.Messages;
    using Kafka.Client.Utils;

    using System.Linq;

    using Kafka.Client.Extensions;

    public abstract class AbstractFetcherThread : ShutdownableThread
    {
        private string clientId;

        private Broker sourceBroker;

        private int socketTimeout;

        private int socketBufferSize;

        private int fetchSize;

        private int fetcherBrokerId;

        private int maxWait;

        private int minBytes;

        public AbstractFetcherThread(
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
            this.fetchRequestBuilder =
                new FetchRequestBuilder().ClientId(clientId)
                                         .ReplicaId(fetcherBrokerId)
                                         .MaxWait(maxWait)
                                         .MinBytes(minBytes);
        }

        private readonly IDictionary<TopicAndPartition, long> partitionMap = new Dictionary<TopicAndPartition, long>();

        private ReentrantLock partitionMapLock;

        private ICondition partitionMapCond;

        protected SimpleConsumer simpleConsumer;

        private string brokerInfo;

        //TODO: metrics and stats

        private FetchRequestBuilder fetchRequestBuilder;

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
            simpleConsumer.Dispose();
        }

        public override void DoWork()
        {
            partitionMapLock.Lock();
            try
            {
                if (partitionMap.Count == 0)
                {
                    partitionMapCond.Await(TimeSpan.FromMilliseconds(200));
                }
                foreach (var topicAndOffset in partitionMap)
                {
                    var topicAndPartition = topicAndOffset.Key;
                    var offset = topicAndOffset.Value;
                    fetchRequestBuilder.AddFetch(topicAndPartition.Topic, topicAndPartition.Partiton, offset, fetchSize);
                }
            }
            finally
            {
                partitionMapLock.Unlock();
            }

            var fetchRequest = fetchRequestBuilder.Build();
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
                Logger.DebugFormat("issuing to broker {0} of fetch request {1}", sourceBroker.Id, fetchRequest);
                response = simpleConsumer.Fetch(fetchRequest);
            }
            catch (Exception e)
            {
                if (isRunning.Get())
                {
                    Logger.Error("Error in fetch " + fetchRequest, e);
                    partitionMapLock.Lock();
                    try
                    {
                        foreach (var key in partitionMap.Keys)
                        {
                            partitionsWithError.Add(key);
                        }
                    }
                    finally
                    {
                        partitionMapLock.Unlock();
                    }
                }
            }

            //TODO: fetcherStats.requestRate.mark()

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
                        if (partitionMap.TryGetValue(topicAndPartition, out currentOffset)
                            && fetchRequest.RequestInfo[topicAndPartition].Offset == currentOffset)
                        {
                            // we append to the log if the current offset is defined and it is the same as the offset requested during fetch                     
                            switch (partitionData.Error)
                            {
                                case ErrorMapping.NoError:
                                    try
                                    {
                                        var messages = (ByteBufferMessageSet)partitionData.Messages;
                                        var validBytes = messages.ValidBytes;
                                        var messageAndOffset =
                                            messages.ShallowEnumerator().ToEnumerable().LastOrDefault();
                                        var newOffset = messageAndOffset != null
                                                            ? messageAndOffset.NextOffset
                                                            : currentOffset;

                                        partitionMap[topicAndPartition] = newOffset;
                                        //TODO: stats

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
                                        ;
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
                                        partitionMap[topicAndPartition] = newOffset;
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
                                            sourceBroker.Id,
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
            partitionMapLock.LockInterruptibly();
            try
            {
                foreach (var topicAndOffset in partitionAndOffsets)
                {
                    var topicAndPartition = topicAndOffset.Key;
                    var offset = topicAndOffset.Value;
                    // If the partitionMap already has the topic/partition, then do not update the map with the old offset
                    if (!partitionMap.ContainsKey(topicAndPartition))
                    {
                        partitionMap[topicAndPartition] = (PartitionTopicInfo.IsOffsetInvalid(offset))
                                                              ? this.HandleOffsetOutOfRange(topicAndPartition)
                                                              : offset;
                    }
                    partitionMapCond.SignalAll();
                }
            }
            finally
            {
                partitionMapLock.Unlock();
            }
        }

        public void RemovePartitions(ISet<TopicAndPartition> topicAndPartitions)
        {
            partitionMapLock.LockInterruptibly();
            try
            {
                foreach (var tp in topicAndPartitions)
                {
                    partitionMap.Remove(tp);
                }

            }
            finally
            {
                partitionMapLock.Unlock();
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
}