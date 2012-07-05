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

namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using System.Threading;
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Messages;
    using Kafka.Client.Requests;
    using Kafka.Client.Utils;
    using Kafka.Client.ZooKeeperIntegration;
    using log4net;

    /// <summary>
    /// Background thread worker class that is used to fetch data from a single broker
    /// </summary>
    internal class FetcherRunnable
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly string name;

        private readonly IZooKeeperClient zkClient;

        private readonly ConsumerConfiguration config;

        private readonly Broker broker;

        private readonly IList<PartitionTopicInfo> partitionTopicInfos;

        private readonly IConsumer simpleConsumer;

        private bool shouldStop;

        internal FetcherRunnable(string name, IZooKeeperClient zkClient, ConsumerConfiguration config, Broker broker, List<PartitionTopicInfo> partitionTopicInfos)
        {
            this.name = name;
            this.zkClient = zkClient;
            this.config = config;
            this.broker = broker;
            this.partitionTopicInfos = partitionTopicInfos;

            this.simpleConsumer = new Consumer(this.config, broker.Host, broker.Port);
        }

        /// <summary>
        /// Method to be used for starting a new thread
        /// </summary>
        internal void Run()
        {
            foreach (var partitionTopicInfo in partitionTopicInfos)
            {
                Logger.InfoFormat(
                    CultureInfo.CurrentCulture,
                    "{0} start fetching topic: {1} part: {2} offset: {3} from {4}:{5}",
                    this.name,
                    partitionTopicInfo.Topic,
                    partitionTopicInfo.PartitionId,
                    partitionTopicInfo.GetFetchOffset(),
                    this.broker.Host,
                    this.broker.Port);
            }
            var reqId = 0;
            try
            {
                while (!this.shouldStop)
                {
                    var builder =
                        new FetchRequestBuilder().
                            CorrelationId(reqId).
                            ClientId(config.ConsumerId ?? this.name).
                            MaxWait(0).
                            MinBytes(0);
                    partitionTopicInfos.ForEach(pti => builder.AddFetch(pti.Topic, pti.PartitionId, pti.GetFetchOffset(), config.FetchSize));

                    var fetchRequest = builder.Build();
                    Logger.Debug("Fetch request: " + fetchRequest);
                    var response = this.simpleConsumer.Fetch(fetchRequest);

                    int read = 0;

                    foreach (PartitionTopicInfo partitionTopicInfo in partitionTopicInfos)
                    {
                        var messages = response.MessageSet(partitionTopicInfo.Topic, partitionTopicInfo.PartitionId);
                        try
                        {
                            bool done = false;
                            if (messages.ErrorCode == ErrorMapping.OffsetOutOfRangeCode)
                            {
                                Logger.InfoFormat("offset for {0} out of range", partitionTopicInfo);
                                //see if we can fix this error
                                var resetOffset = ResetConsumerOffsets(partitionTopicInfo.Topic,
                                                                       partitionTopicInfo.PartitionId);
                                if (resetOffset >= 0)
                                {
                                    partitionTopicInfo.ResetFetchOffset(resetOffset);
                                    partitionTopicInfo.ResetConsumeOffset(resetOffset);
                                    done = true;
                                }
                            }
                            if (!done)
                            {
                                read += partitionTopicInfo.Add(messages, partitionTopicInfo.GetFetchOffset());
                            }
                        }
                        catch (Exception ex)
                        {
                            if (!shouldStop)
                            {
                                Logger.ErrorFormat("error in FetcherRunnable for {0}", partitionTopicInfo, ex);
                            }
                        }
                    }
                    reqId = reqId == int.MaxValue ? 0 : reqId + 1;

                    Logger.Info("Fetched bytes: " + read);
                    if (read == 0)
                    {
                        Logger.DebugFormat(CultureInfo.CurrentCulture, "backing off {0} ms", this.config.BackOffIncrement);
                        Thread.Sleep(this.config.BackOffIncrement);
                    }


                    //var items = this.partitionTopicInfos.Zip(
                    //    response,
                    //    (x, y) =>
                    //    new Tuple<PartitionTopicInfo, BufferedMessageSet>(x, y));
                    //foreach (Tuple<PartitionTopicInfo, BufferedMessageSet> item in items)
                    //{
                    //    BufferedMessageSet messages = item.Item2;
                    //    PartitionTopicInfo info = item.Item1;
                    //    try
                    //    {
                    //        bool done = false;
                    //        if (messages.ErrorCode == ErrorMapping.OffsetOutOfRangeCode)
                    //        {
                    //            Logger.InfoFormat(CultureInfo.CurrentCulture, "offset {0} out of range", info.GetFetchOffset());
                    //            //// see if we can fix this error
                    //            var resetOffset = this.ResetConsumerOffsets(info.Topic, info.PartitionId);
                    //            if (resetOffset >= 0)
                    //            {
                    //                info.ResetFetchOffset(resetOffset);
                    //                info.ResetConsumeOffset(resetOffset);
                    //                done = true;
                    //            }
                    //        }

                    //        if (!done)
                    //        {
                    //            read += info.Add(messages, info.GetFetchOffset());
                    //        }
                    //    }
                    //    catch (Exception ex)
                    //    {
                    //        if (!shouldStop)
                    //        {
                    //            Logger.ErrorFormat(CultureInfo.CurrentCulture, "error in FetcherRunnable for {0}" + info, ex);
                    //        }

                    //        throw;
                    //    }
                    //}

                    //Logger.Info("Fetched bytes: " + read);
                    //if (read == 0)
                    //{
                    //    Logger.DebugFormat(CultureInfo.CurrentCulture, "backing off {0} ms", this.config.BackOffIncrement);
                    //    Thread.Sleep(this.config.BackOffIncrement);
                    //}
                }
            }
            catch (Exception ex)
            {
                if (shouldStop)
                {
                    Logger.InfoFormat(CultureInfo.CurrentCulture, "FetcherRunnable {0} interrupted", this);
                }
                else
                {
                    Logger.ErrorFormat(CultureInfo.CurrentCulture, "error in FetcherRunnable {0}", ex);
                }
            }

            Logger.InfoFormat(CultureInfo.CurrentCulture, "stopping fetcher {0} to host {1}", this.name, this.broker.Host);

        }

        internal void Shutdown()
        {
            this.shouldStop = true;
        }

        private long ResetConsumerOffsets(string topic, int partitionId)
        {
            long offset;
            switch (this.config.AutoOffsetReset)
            {
                case OffsetRequest.SmallestTime:
                    offset = OffsetRequest.EarliestTime;
                    break;
                case OffsetRequest.LargestTime:
                    offset = OffsetRequest.LatestTime;
                    break;
                default:
                    return -1;
            }

            var request = new OffsetRequest(topic, partitionId, offset, 1);
            var offsets = this.simpleConsumer.GetOffsetsBefore(request);
            var topicDirs = new ZKGroupTopicDirs(this.config.GroupId, topic);
            Logger.InfoFormat(CultureInfo.CurrentCulture, "updating partition {0} with {1} offset {2}", partitionId, offset == OffsetRequest.EarliestTime ? "earliest" : "latest", offsets[0]);
            ZkUtils.UpdatePersistentPath(this.zkClient, topicDirs.ConsumerOffsetDir + "/" + partitionId, offsets[0].ToString());

            return offsets[0];
        }
    }
}
