/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Collections.Concurrent;
using System.ComponentModel;
using System.Reflection;
using System.Threading;
using Kafka.Client.Exceptions;
using log4net;

namespace Kafka.Client.Producers.Async
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class ProducerSendAsyncWorker<TKey, TData>
    {
        private string threadName;

        private BlockingCollection<ProducerData<TKey, TData>> queue;

        private ICallbackHandler<TKey, TData> handler;

        private int queueTime;

        private int batchSize;

        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private new ProducerData<TKey, TData> shutdownCommand = new ProducerData<TKey, TData>(null, default(TKey), null);

        private BackgroundWorker backgroundWorker;

        private static ManualResetEvent mre = new ManualResetEvent(false);

        public ProducerSendAsyncWorker(string threadName, BlockingCollection<ProducerData<TKey, TData>> queue, ICallbackHandler<TKey, TData> handler, int queueTime, int batchSize)
        {
            this.threadName = threadName;
            this.queue = queue;
            this.handler = handler;
            this.queueTime = queueTime;
            this.batchSize = batchSize;
            this.backgroundWorker = new BackgroundWorker();
            this.backgroundWorker.DoWork += new DoWorkEventHandler(backgroundWorker_DoWork);
            this.backgroundWorker.RunWorkerAsync();
        }

        public void Shutdown()
        {
            Logger.Info("Beginning shutting down ProducerSendAsyncWorker");
            this.queue.Add(this.shutdownCommand);
            mre.WaitOne();
            Logger.Info("Shutdown ProducerSendAsyncWorker complete");
        }

        void backgroundWorker_DoWork(object sender, DoWorkEventArgs e)
        {
            try
            {
                mre.Reset();
                BackgroundWorker worker = sender as BackgroundWorker;
                var lastSend = DateTime.Now;
                var events = new List<ProducerData<TKey, TData>>();
                bool full = false;

                //drain the queue until you get a shutdown command
                while (true)
                {
                    ProducerData<TKey, TData> currentQueueItem;
                    queue.TryTake(out currentQueueItem,
                                  Math.Max(0, (lastSend + new TimeSpan(0, 0, 0, 0, queueTime) - DateTime.Now).Milliseconds));
                    if (currentQueueItem != null && this.IsShutdownCommand(currentQueueItem))
                    {
                        break;
                    }
                    var elapsed = DateTime.Now - lastSend;
                    //check if the queue time is reached. This happens when the take method above returns after a timeout and returns a null object
                    var expired = currentQueueItem == null;
                    if (currentQueueItem != null)
                    {
                        if (currentQueueItem.Key == null)
                        {
                            Logger.InfoFormat("Dequeued item for topic {0}, no partition key, data: {1}",
                                              currentQueueItem.Topic, string.Join(", ", currentQueueItem.Data));
                        }
                        else
                        {
                            Logger.InfoFormat("Dequeued item for topic {0}, partition key: {1}, data: {2}",
                                              currentQueueItem.Topic, currentQueueItem.Key, string.Join(", ", currentQueueItem.Data));
                        }
                        events.Add(currentQueueItem);

                        //check if the batch size is reached
                        full = events.Count >= this.batchSize;
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
                        events = new List<ProducerData<TKey, TData>>();
                    }
                }
                if (queue.Count > 0)
                {
                    throw new IllegalQueueStateException(
                        string.Format("Invalid queue state! After queue shutdown, {0} remaining items in the queue",
                                      queue.Count));
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

        private bool IsShutdownCommand(ProducerData<TKey, TData> data)
        {
            return data.Topic == shutdownCommand.Topic && data.Key.Equals(shutdownCommand.Key) &&
                   data.Data == shutdownCommand.Data;
        }

        private void TryToHandle(IEnumerable<ProducerData<TKey, TData>> events)
        {
            try
            {
                Logger.DebugFormat("Handling {0} events", events.Count());
                if (events.Count() > 0)
                {
                    this.handler.Handle(events);
                }
            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("Error in handling batch of {0} events", events.Count(), ex);
            }
        }
    }
}
