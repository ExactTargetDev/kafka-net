using System;

namespace Kafka.Client.Consumers
{
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Reflection;
    using System.Threading;

    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Messages;
    using Kafka.Client.Serializers;
    using Kafka.Client.Utils;

    using log4net;

    /// <summary>
    ///  An iterator that blocks until a value can be read from the supplied queue.
    /// The iterator takes a shutdownCommand object which can be added to the queue to trigger a shutdown
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    internal class ConsumerIterator<TKey, TValue>: IteratorTemplate<MessageAndMetadata<TKey, TValue>>
    {
        private readonly BlockingCollection<FetchedDataChunk> channel;

        private readonly int consumerTimeoutMs;

        private IDecoder<TKey> keyDecoder;
        private IDecoder<TValue> valueDecoder;

        public string ClientId { get; set; }

        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        
        private AtomicReference<IEnumerator<MessageAndOffset>> current = new AtomicReference<IEnumerator<MessageAndOffset>>(null);

        private PartitionTopicInfo currentTopicInfo;

        private long consumedOffset = -1;

        public ConsumerIterator(BlockingCollection<FetchedDataChunk> channel, int consumerTimeoutMs, IDecoder<TKey> keyDecoder, IDecoder<TValue> valueDecoder, string clientId)
        {
            this.channel = channel;
            this.consumerTimeoutMs = consumerTimeoutMs;
            this.keyDecoder = keyDecoder;
            this.valueDecoder = valueDecoder;
            this.ClientId = clientId;
        }

        public override MessageAndMetadata<TKey, TValue> Current
        {
            get
            {
                var item = base.Current;
                if (consumedOffset < 0)
                {
                    throw new KafkaException(string.Format("Offset returned by the message set is invalid {0}", consumedOffset));
                }
                currentTopicInfo.ResetConsumeOffset(consumedOffset);
                var topic = currentTopicInfo.Topic;
                Logger.DebugFormat("Setting {0} consumer offset to {1}", topic, consumedOffset);
                //TODO: consumerTopicStats.getConsumerTopicStats(topic).messageRate.mark()
                 //TODO: consumerTopicStats.getConsumerAllTopicStats().messageRate.mark()
                return item;
            }
        }

        protected override MessageAndMetadata<TKey, TValue> MakeNext()
        {
            FetchedDataChunk currentDataChunk = null;
            var localCurrent = current.Get();
            if (localCurrent == null || !localCurrent.MoveNext())
            {
                if (consumerTimeoutMs < 0)
                {
                    currentDataChunk = channel.Take();
                }
                else
                {
                    if (!channel.TryTake(out currentDataChunk, consumerTimeoutMs))
                    {
                         // reste stat to make the iterator re-iterable
                        Reset();
                        throw new ConsumerTimeoutException();
                    } 
                }

                if (currentDataChunk.Equals(ZookeeperConsumerConnector.ShutdownCommand))
                {
                    Logger.Debug("Received the shutdown command");
                    channel.Add(currentDataChunk);
                    return this.AllDone();
                }
                else
                {
                    currentTopicInfo = currentDataChunk.TopicInfo;
                    var cdcFetchOffset = currentDataChunk.FetchOffset;
                    var ctiConsumeOffset = currentTopicInfo.GetFetchOffset();
                    Logger.DebugFormat(
                        "CurrentTopicInfo: ConsumedOffset({0}), FetchOffset({1})",
                        currentTopicInfo.GetConsumeOffset(),
                        currentTopicInfo.GetFetchOffset());

                    if (ctiConsumeOffset < cdcFetchOffset)
                    {
                        Logger.ErrorFormat(
                            CultureInfo.CurrentCulture,
                            "consumed offset: {0} doesn't match fetch offset: {1} for {2}; consumer may lose data",
                            ctiConsumeOffset,
                            cdcFetchOffset,
                            currentTopicInfo);
                        currentTopicInfo.ResetConsumeOffset(currentDataChunk.FetchOffset);
                    }
                    localCurrent = currentDataChunk.Messages.GetEnumerator();
                    current.Set(localCurrent);
                }

                 // if we just updated the current chunk and it is empty that means the fetch size is too small!
                if(currentDataChunk.Messages.ValidBytes == 0)
                     throw new MessageSizeTooLargeException(string.Format("Found a message larger than the maximum fetch size of this consumer on topic " +
                                               "{0} partition {1} at fetch offset {2}. Increase the fetch size, or decrease the maximum message size the broker will allow.",
                                               currentDataChunk.TopicInfo.Topic, currentDataChunk.TopicInfo.PartitionId, currentDataChunk.FetchOffset));
            }

            var item = localCurrent.Current;
            // reject the messages that have already been consumed
            while (item.Offset < currentTopicInfo.GetConsumeOffset() && localCurrent.MoveNext())
            {
                item = localCurrent.Current;
            }

            item.Message.EnsureValid(); // validate checksum of message to ensure it is valid

            return new MessageAndMetadata<TKey, TValue>(
                currentTopicInfo.Topic,
                currentTopicInfo.PartitionId,
                item.Message,
                item.Offset,
                keyDecoder,
                valueDecoder);

        }


        //TODO: private val consumerTopicStats = ConsumerTopicStatsRegistry.getConsumerTopicStat(clientId)


        public void ClearCurrentChunk()
        {
            Logger.Debug("Clearing the current data chunk for this consumer iterator");
            current.Set(null);
        }


    }
}

internal class ConsumerTimeoutException : Exception
{
    
}