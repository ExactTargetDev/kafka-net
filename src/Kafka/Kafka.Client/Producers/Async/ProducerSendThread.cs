namespace Kafka.Client.Producers.Async
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Linq;
    using System.Reflection;
    using System.Threading;

    using log4net;
    
    public class ProducerSendThread<TKey, TValue>
    {
        public string ThreadName { get; private set; }

        public BlockingCollection<KeyedMessage<TKey, TValue>> Queue { get; private set; }

        public IEventHandler<TKey, TValue> Handler { get; private set; }

        public int QueueTime { get; private set; }

        public int BatchSize { get; private set; }

        public string ClientId { get; private set; }

        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private static ManualResetEvent mre = new ManualResetEvent(false);
        private KeyedMessage<TKey, TValue> shutdownCommand = new KeyedMessage<TKey, TValue>("shutdown", default(TKey), default(TValue));

        private BackgroundWorker backgroundWorker;

        public ProducerSendThread(string threadName, BlockingCollection<KeyedMessage<TKey, TValue>> queue, IEventHandler<TKey, TValue> handler, int queueTime, int batchSize, string clientId)
        {
            this.ThreadName = threadName;
            this.Queue = queue;
            this.Handler = handler;
            this.QueueTime = queueTime;
            this.BatchSize = batchSize;
            this.ClientId = clientId;
            this.backgroundWorker = new BackgroundWorker();
            this.backgroundWorker.DoWork += new DoWorkEventHandler(Run);
        }


        void Run(object sender, DoWorkEventArgs e)
        {
            try
            {
                mre.Reset();
                var lastSend = DateTime.Now;
                var events = new List<KeyedMessage<TKey, TValue>>();
                bool full = false;

                // drain the queue until you get a shutdown command
                while (true)
                {
                    KeyedMessage<TKey, TValue> currentQueueItem;
                    Queue.TryTake(out currentQueueItem,
                                  Math.Max(0, Convert.ToInt32((lastSend.AddMilliseconds(QueueTime) - DateTime.Now).TotalMilliseconds)));
                    if (currentQueueItem != null && this.IsShutdownCommand(currentQueueItem))
                    {
                        break;
                    }
                    var elapsed = DateTime.Now - lastSend;

                    // check if the queue time is reached. This happens when the take method above returns after a timeout and returns a null object
                    var expired = currentQueueItem == null;
                    if (currentQueueItem != null)
                    {
                            Logger.InfoFormat("Dequeued item for topic {0}, partition key: {1}, Data: {2}",
                                              currentQueueItem.Topic, 
                                              currentQueueItem.Key, 
                                              string.Join(", ", currentQueueItem.Message));
                        events.Add(currentQueueItem);

                        //check if the batch size is reached
                        full = events.Count >= this.BatchSize;
                    }

                    if (full || expired)
                    {
                        if (expired)
                        {
                            Logger.DebugFormat("{0}ms elapsed. Queue time reached. Sending...", elapsed.TotalMilliseconds);
                        }
                        if (full)
                        {
                            Logger.Debug("Batch full. Sending...");
                        }
                        this.TryToHandle(events);
                        lastSend = DateTime.Now;
                        events = new List<KeyedMessage<TKey, TValue>>();
                    }
                }

                this.TryToHandle(events);

                if (Queue.Count > 0)
                {
                    throw new IllegalQueueStateException(
                        string.Format("Invalid queue state! After queue shutdown, {0} remaining items in the queue",
                                      Queue.Count));
                }
            }
            catch (Exception ex)
            {
                Logger.Error("Error in sending events: ", ex);
            }
            finally
            {
                mre.Set();
            }
        }

        private bool IsShutdownCommand(KeyedMessage<TKey, TValue> data)
        {
            return data.Equals(this.shutdownCommand);
        }

        private void TryToHandle(IEnumerable<KeyedMessage<TKey, TValue>> events)
        {
            try
            {
                Logger.DebugFormat("Handling {0} events", events.Count());
                if (events.Count() > 0)
                {
                    this.Handler.Handle(events);
                }
            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("Error in handling batch of {0} events", events.Count(), ex);
            }
        }

        public void Shutdown()
        {
            Logger.Info("Beginning shutting down ProducerSendAsyncWorker");
            this.Queue.Add(this.shutdownCommand);
            mre.WaitOne();
            Logger.Info("Shutdown ProducerSendAsyncWorker complete");
        }

        public void Start()
        {
            this.backgroundWorker.RunWorkerAsync();
        }
    }
}