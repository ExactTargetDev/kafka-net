namespace Kafka.Client.Producers.Async
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Linq;
    using System.Reflection;

    using Kafka.Client.Common;

    using log4net;

    using Spring.Threading;

    public class ProducerSendThread<TKey, TValue>
    {
        public string ThreadName { get; private set; }

        public BlockingCollection<KeyedMessage<TKey, TValue>> Queue { get; private set; }

        public IEventHandler<TKey, TValue> Handler { get; private set; }

        public int QueueTime { get; private set; }

        public int BatchSize { get; private set; }

        public string ClientId { get; private set; }

        private readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly CountDownLatch shutdownLatch = new CountDownLatch(1);

        private readonly KeyedMessage<TKey, TValue> shutdownCommand = new KeyedMessage<TKey, TValue>("shutdown", default(TKey), default(TValue));

        private readonly BackgroundWorker backgroundWorker;

        public ProducerSendThread(string threadName, BlockingCollection<KeyedMessage<TKey, TValue>> queue, IEventHandler<TKey, TValue> handler, int queueTime, int batchSize, string clientId)
        {
            this.ThreadName = threadName;
            this.Queue = queue;
            this.Handler = handler;
            this.QueueTime = queueTime;
            this.BatchSize = batchSize;
            this.ClientId = clientId;
            this.backgroundWorker = new BackgroundWorker();
            this.backgroundWorker.DoWork += this.Run;

            MetersFactory.NewGauge(clientId + "-ProducerQueueSize", () => this.Queue.Count);
        }

        internal void Run(object sender, DoWorkEventArgs args)
        {
            try
            {
                this.ProcessEvents();
            }
            catch (Exception e)
            {
                this.Logger.Error("error in sending events", e);
            }
            finally
            {
                this.shutdownLatch.CountDown();
            }
        }

        public void Shutdown()
        {
            this.Logger.Info("Beginning shutting down ProducerSendAsyncWorker");
            this.Queue.Add(this.shutdownCommand);
            this.shutdownLatch.Await();
            this.Logger.Info("Shutdown ProducerSendAsyncWorker complete");
        }

        private void ProcessEvents() 
        {
            var lastSend = DateTime.Now;
            var events = new List<KeyedMessage<TKey, TValue>>();
            bool full;

            // drain the queue until you get a shutdown command
            while (true)
            {
                KeyedMessage<TKey, TValue> currentQueueItem;
                this.Queue.TryTake(
                    out currentQueueItem,
                                Math.Max(0, Convert.ToInt32((lastSend.AddMilliseconds(this.QueueTime) - DateTime.Now).TotalMilliseconds)));
                if (currentQueueItem != null && this.shutdownCommand.Equals(currentQueueItem))
                {
                    break;
                }
                var elapsed = DateTime.Now - lastSend;

                // check if the queue time is reached. This happens when the take method above returns after a timeout and returns a null object
                var expired = currentQueueItem == null;
                if (currentQueueItem != null)
                {
                        this.Logger.InfoFormat(
                            "Dequeued item for topic {0}, partition key: {1}, Data: {2}",
                                            currentQueueItem.Topic, 
                                            currentQueueItem.Key, 
                                            string.Join(", ", currentQueueItem.Message));
                    events.Add(currentQueueItem);
                }

                // check if the batch size is reached
                full = events.Count >= this.BatchSize;

                if (full || expired)
                {
                    if (expired)
                    {
                        this.Logger.DebugFormat("{0}ms elapsed. Queue time reached. Sending...", elapsed.TotalMilliseconds);
                    }

                    if (full)
                    {
                        this.Logger.Debug("Batch full. Sending...");
                    }

                    this.TryToHandle(events);
                    lastSend = DateTime.Now;
                    events = new List<KeyedMessage<TKey, TValue>>();
                }
            }

            this.TryToHandle(events);

            if (this.Queue.Count > 0)
            {
                throw new IllegalQueueStateException(
                    string.Format(
                    "Invalid queue state! After queue shutdown, {0} remaining items in the queue",
                                    this.Queue.Count));
            }
        }

        private void TryToHandle(IEnumerable<KeyedMessage<TKey, TValue>> events)
        {
            var size = events.Count();
            try
            {
                this.Logger.DebugFormat("Handling {0} events", size);
                if (size > 0)
                {
                    this.Handler.Handle(events);
                }
            }
            catch (Exception ex)
            {
                this.Logger.Error(string.Format("Error in handling batch of {0} events", size), ex);
            }
        }

        public void Start()
        {
            this.backgroundWorker.RunWorkerAsync();
        }
    }
}