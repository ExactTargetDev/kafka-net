namespace Kafka.Client.Producers
{
    using System;
    using System.Collections.Concurrent;
    using System.Reflection;

    using Kafka.Client.Cfg;
    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Producers.Async;

    using log4net;

    public class Producer<TKey, TValue> : IDisposable
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly ProducerConfiguration config;

        private readonly IEventHandler<TKey, TValue> eventHandler;

        public Producer(ProducerConfiguration config, IEventHandler<TKey, TValue> eventHandler)
         {
             this.config = config;
             this.eventHandler = eventHandler;

             if (config.ProducerType == ProducerTypes.Async)
             {
                 this.sync = false;
                 this.producerSendThread = new ProducerSendThread<TKey, TValue>("ProducerSendThread-" + config.ClientId, queue, eventHandler, config.QueueBufferingMaxMs, config.BatchNumMessages, config.ClientId);
                 this.producerSendThread.Start();
             }
         }

        private readonly AtomicBoolean hasShutdown = new AtomicBoolean(false);

        private readonly BlockingCollection<KeyedMessage<TKey, TValue>> queue = null;

        private bool sync = true;
        private ProducerSendThread<TKey, TValue> producerSendThread = null;
        private object lockObject = new object();

        //TODO: private val producerTopicStats = ProducerTopicStatsRegistry.getProducerTopicStats(config.clientId)
        //TODO: KafkaMetricsReporter.startReporters(config.props)

        /* TODO public Producer(ProducerConfig config) : this(config, new DefaultEventHandler<TKey, TValue>(
            config,
                                      Utils.createObject<Partitioner>(config.partitionerClass, config.props),
                                      Utils.createObject<IEncoder<V>>(config.serializerClass, config.props),
                                      Utils.createObject<IEncoder<K>>(config.keySerializerClass, config.props),
                                      new ProducerPool(config))
            ))
        {
            
        }*/

        public void Send(params KeyedMessage<TKey, TValue>[] messages)
        {
            lock (lockObject)
            {
                if (hasShutdown.Get())
                {
                    throw new ProducerClosedException();
                }
                //TODO: record stats

                if (sync)
                {
                    eventHandler.handle(messages);
                }
                else
                {
                    asyncSend(messages);
                }
            }
        }

        public void RecordStats(KeyedMessage<TKey, TValue>[] messages)
        {
            foreach (var message in messages)
            {
               ///TODO: 
               ///  producerTopicStats.getProducerTopicStats(message.topic).messageRate.mark()
      //producerTopicStats.getProducerAllTopicsStats.messageRate.mark()
            }
        }

        private void AsyncSend(KeyedMessage<TKey, TValue>[] messages)
        {
            /* TODO:
             *  for (message <- messages) {
      val added = config.queueEnqueueTimeoutMs match {
        case 0  =>
          queue.offer(message)
        case _  =>
          try {
            config.queueEnqueueTimeoutMs < 0 match {
            case true =>
              queue.put(message)
              true
            case _ =>
              queue.offer(message, config.queueEnqueueTimeoutMs, TimeUnit.MILLISECONDS)
            }
          }
          catch {
            case e: InterruptedException =>
              false
          }
      }
      if(!added) {
        producerTopicStats.getProducerTopicStats(message.topic).droppedMessageRate.mark()
        producerTopicStats.getProducerAllTopicsStats.droppedMessageRate.mark()
        throw new QueueFullException("Event queue is full of unsent messages, could not send event: " + message.toString)
      }else {
        trace("Added to send queue an event: " + message.toString)
        trace("Remaining queue size: " + queue.remainingCapacity)
      }
    }*/
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