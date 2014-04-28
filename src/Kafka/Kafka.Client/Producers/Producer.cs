namespace Kafka.Client.Producers
{
    using System;
    using System.Collections.Concurrent;
    using System.Reflection;

    using Kafka.Client.Cfg;
    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Producers.Async;
    using Kafka.Client.Serializers;
    using Kafka.Client.Utils;

    using log4net;

    public class Producer<TKey, TValue> : IDisposable
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly ProducerConfig config;

        private readonly IEventHandler<TKey, TValue> eventHandler;

        public Producer(ProducerConfig config) : this(config, new DefaultEventHandler<TKey, TValue>(config, 
                Util.CreateObject<IPartitioner>(config.PartitionerClass, config), 
                Util.CreateObject<IEncoder<TValue>>(config.Serializer, config),
                Util.CreateObject<IEncoder<TKey>>(config.KeySerializer, config),
                new ProducerPool(config))) 
        {
        }

        public Producer(ProducerConfig config, IEventHandler<TKey, TValue> eventHandler)
         {
             this.config = config;
             this.eventHandler = eventHandler;

             this.queue = new BlockingCollection<KeyedMessage<TKey, TValue>>(config.QueueBufferingMaxMessages);

             if (config.ProducerType == ProducerTypes.Async)
             {
                 this.sync = false;
                 this.producerSendThread = new ProducerSendThread<TKey, TValue>("ProducerSendThread-" + config.ClientId, queue, eventHandler, config.QueueBufferingMaxMs, config.BatchNumMessages, config.ClientId);
                 this.producerSendThread.Start();
             }

            this.producerTopicStats = ProducerTopicStatsRegistry.GetProducerTopicStats(config.ClientId);
         }

        private readonly AtomicBoolean hasShutdown = new AtomicBoolean(false);

        private readonly BlockingCollection<KeyedMessage<TKey, TValue>> queue = null;

        private bool sync = true;
        private ProducerSendThread<TKey, TValue> producerSendThread = null;
        private object lockObject = new object();

        private readonly ProducerTopicStats producerTopicStats;

        //TODO: KafkaMetricsReporter.startReporters(config.props)

        public void Send(params KeyedMessage<TKey, TValue>[] messages)
        {
            lock (lockObject)
            {
                if (hasShutdown.Get())
                {
                    throw new ProducerClosedException();
                }

                this.RecordStats(messages);

                if (sync)
                {
                    eventHandler.Handle(messages);
                }
                else
                {
                    this.AsyncSend(messages);
                }
            }
        }

        public void RecordStats(KeyedMessage<TKey, TValue>[] messages)
        {
            foreach (var message in messages)
            {
                producerTopicStats.GetProducerTopicStats(message.Topic).MessageRate.Mark();
                producerTopicStats.GetProducerAllTopicsStats().MessageRate.Mark();
            }
        }

        private void AsyncSend(KeyedMessage<TKey, TValue>[] messages)
        {
            foreach (KeyedMessage<TKey, TValue> message in messages)
            {
                bool added;
                switch (this.config.QueueEnqueueTimeoutMs)
                {
                    case 0:
                        added = this.queue.TryAdd(message);
                        break;
                    default:
                        try
                        {
                            if (this.config.QueueEnqueueTimeoutMs < 0)
                            {
                                this.queue.Add(message);
                                added = true;
                            }
                            else
                            {
                                added = this.queue.TryAdd(message, config.QueueEnqueueTimeoutMs);
                            }
                        }
                        catch (Exception ex)
                        {
                            Logger.Error("Error in AsyncSend", ex);
                            added = false;
                        }
                        break;
                }
                if (!added)
                {
                    producerTopicStats.GetProducerTopicStats(message.Topic).DroppedMessageRate.Mark();
                    producerTopicStats.GetProducerAllTopicsStats().DroppedMessageRate.Mark();
                    throw new QueueFullException(
                        "Event queue is full of unsent messages, could not send event: " + message);
                }
                else
                {
                    if (Logger.IsDebugEnabled)
                    {
                        Logger.Debug("Added to send queue an event: " + message);
                        Logger.Debug("Remaining queue size: " + (this.queue.BoundedCapacity - this.queue.Count));
                    }
                }
               
            }
        }

        /// <summary>
        /// Close API to close the producer pool connections to all Kafka brokers. Also closes
        /// the zookeeper client connection if one exists
        /// </summary>
        public void Dispose()
        {
            lock (this.lockObject)
            {
                var canShutdown = this.hasShutdown.CompareAndSet(false, true);
                if (canShutdown)
                {
                    Logger.Info("Shutting down producer");
                    if (this.producerSendThread != null)
                    {
                        this.producerSendThread.Shutdown();
                    }
                    this.eventHandler.Dispose();
                }
            }
        }
    }
}