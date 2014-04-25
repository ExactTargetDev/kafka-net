namespace Kafka.Client.Consumers
{
    using System.Collections.Concurrent;
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using System.Threading;

    using Kafka.Client.Common.Imported;
    using Kafka.Client.Messages;

    using log4net;

    public class PartitionTopicInfo
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly BlockingCollection<FetchedDataChunk> chunkQueue;

        private AtomicLong consumedOffset;

        private AtomicLong fetchedOffset;

        private AtomicInteger fetchSize;

        private readonly string clientId;

        public const long InvalidOffset = -1L;

        public static bool IsOffsetInvalid(long offset)
        {
            return offset < 0L;
        }


        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionTopicInfo"/> class.
        /// </summary>
        /// <param name="topic">
        /// The topic.
        /// </param>
        /// <param name="partitionId">
        /// The broker's partition.
        /// </param>
        /// <param name="chunkQueue">
        /// The chunk queue.
        /// </param>
        /// <param name="consumedOffset">
        /// The consumed offset value.
        /// </param>
        /// <param name="fetchedOffset">
        /// The fetched offset value.
        /// </param>
        /// <param name="fetchSize">
        /// The fetch size.
        /// </param>
        /// <param name="clientId">Client id</param>
        public PartitionTopicInfo(
            string topic,
            int partitionId,
            BlockingCollection<FetchedDataChunk> chunkQueue,
            AtomicLong consumedOffset,
            AtomicLong fetchedOffset,
            AtomicInteger fetchSize,
            string clientId)
        {
            this.Topic = topic;
            this.PartitionId = partitionId;
            this.chunkQueue = chunkQueue;
            this.consumedOffset = consumedOffset;
            this.fetchedOffset = fetchedOffset;
            this.fetchSize = fetchSize;
            this.clientId = clientId;
            consumerTopicStats = ConsumerTopicStatsRegistry.GetConsumerTopicStat(clientId);
            if (Logger.IsDebugEnabled)
            {
                Logger.DebugFormat(
                    CultureInfo.CurrentCulture, "initial consumer offset of {0} is {1}", this, consumedOffset);
                Logger.DebugFormat(
                    CultureInfo.CurrentCulture, "initial fetch offset of {0} is {1}", this, fetchedOffset);
            }
        }

        /// <summary>
        /// Gets the partition Id.
        /// </summary>
        public int PartitionId { get; private set; }

        /// <summary>
        /// Gets the topic.
        /// </summary>
        public string Topic { get; private set; }

        private readonly ConsumerTopicStats consumerTopicStats;

        public long GetConsumeOffset()
        {
            return this.consumedOffset.Get();
        }

        public long GetFetchOffset()
        {
            return this.fetchedOffset.Get();
        }

        public void ResetConsumeOffset(long newConsumeOffset)
        {
            this.consumedOffset.Set(newConsumeOffset);

            if (Logger.IsDebugEnabled)
            {
                Logger.DebugFormat(
                    CultureInfo.CurrentCulture, "reset consume offset of {0} to {1}", this, newConsumeOffset);
            }
        }

        public void ResetFetchOffset(long newFetchOffset)
        {
            this.fetchedOffset.Set(newFetchOffset);

            if (Logger.IsDebugEnabled)
            {
                Logger.DebugFormat(
                    CultureInfo.CurrentCulture, "reset fetch offset of {0} to {1}", this, newFetchOffset);
            }
        }

        public void Enqueue(ByteBufferMessageSet messages)
        {
            var size = messages.ValidBytes;
            if (size > 0)
            {

                var next = 0; //TODO: var next = messages.ShallowEnumerator().Last().NextOffset;
                Logger.DebugFormat("Updating fetch offset = {0} to {1}", this.fetchedOffset.Get(), next);
                chunkQueue.Add(new FetchedDataChunk(messages, this, this.fetchedOffset.Get()));
                fetchedOffset.Set(next);
                Logger.DebugFormat("Updated fetch offset of {0} to {1}", this, next);
                //TODO: consumerTopicStats.GetConsumerTopicStats(Topic).ByteRate.Mark(size);
                //TODO:consumerTopicStats.GetConsumerAllTopicStats().ByteRate.Mark(size);

            }
            else if (messages.SizeInBytes > 0)
            {
                this.chunkQueue.Add(new FetchedDataChunk(messages, this, this.fetchedOffset.Get()));
            }
        }

        public override string ToString()
        {
            return string.Format("{0}:{1}: fetched offset = {2}: consumed offset = {3}", this.Topic, this.PartitionId, this.fetchedOffset, this.consumedOffset);
        }
    }
}