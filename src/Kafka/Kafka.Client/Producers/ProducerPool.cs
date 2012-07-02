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

using System.Reflection;
using Kafka.Client.Exceptions;
using Kafka.Client.ZooKeeperIntegration;
using log4net;

namespace Kafka.Client.Producers
{
    using System;
    using System.Collections.Generic;
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Producers.Async;
    using Kafka.Client.Producers.Sync;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using System.Linq;

    /// <summary>
    /// The base for all classes that represents pool of producers used by high-level API
    /// </summary>
    /// <typeparam name="TData">The type of the data.</typeparam>
    internal class ProducerPool// : IProducerPool
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        protected bool Disposed { get; set; }

        private Dictionary<int, SyncProducer> syncProducers;

        protected ProducerConfiguration Config { get; private set; }

        private ZooKeeperClient zkClient;

        private object myLock = new object();

        private Random random = new Random();

        /// <summary>
        /// Initializes a new instance of the <see cref="ProducerPool&lt;TData&gt;"/> class.
        /// </summary>
        /// <param name="config">The config.</param>
        /// <param name="serializer">The serializer.</param>
        /// <remarks>
        /// Should be used for testing purpose only
        /// </remarks>
        internal ProducerPool(
            ProducerConfiguration config,
            ZooKeeperClient zkClient)
        {
            Guard.NotNull(config, "config");
            Guard.NotNull(zkClient, "zkClient");

            this.syncProducers = new Dictionary<int, SyncProducer>();
            this.Config = config;
            this.zkClient = zkClient;
            this.zkClient.Connect();
        }

        
        public void AddProducer(Broker broker)
        {
            var syncProducerConfig = new SyncProducerConfiguration(this.Config, broker.Id, broker.Host, broker.Port);
            var producer = new SyncProducer(syncProducerConfig);
            Logger.InfoFormat("Creating sync producer for broker id = {0} at {1}:{2}", broker.Id, broker.Host, broker.Port);
            this.syncProducers.Add(broker.Id, producer);
        }

        public void AddProducers(ProducerConfiguration config)
        {
            lock (myLock)
            {
                Logger.DebugFormat("Connecting to {0} for creating sync producers for all brokers in the cluster", config.ZooKeeper.ZkConnect);
                var brokers = ZkUtils.GetAllBrokersInCluster(this.zkClient);
                brokers.ForEach(this.AddProducer);
            }
        }

        public SyncProducer GetProducer(int brokerId)
        {
            lock (myLock)
            {
                if (!this.syncProducers.ContainsKey(brokerId))
                {
                    throw new UnavailableProducerException(
                        string.Format("Sync producer for broker id {0} does not exist", brokerId));
                }
                return this.syncProducers[brokerId];
            }
        }

        public SyncProducer GetAnyProducer()
        {
            lock (myLock)
            {
                if (this.syncProducers.Count == 0)
                {
                    //refresh the list of brokers from zookeeper
                    Logger.Info("No sync producers available. Refreshing the available broker list from ZK and creating sync producers");
                    this.AddProducers(this.Config);
                    if (this.syncProducers.Count == 0)
                    {
                        throw new NoBrokersForPartitionException("No brokers available");
                    }
                }
                return this.syncProducers.ElementAt(random.Next(this.syncProducers.Count)).Value;
            }
        }

        public ZooKeeperClient GetZkClient()
        {
            return this.zkClient;
        }

        /// <summary>
        /// Releases all unmanaged and managed resources
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected void Dispose(bool disposing)
        {
            if (!disposing)
            {
                return;
            }

            if (this.Disposed)
            {
                return;
            }

            this.Disposed = true;
            this.syncProducers.ForEach(x => x.Value.Dispose());
        }

        /// <summary>
        /// Ensures that object was not disposed
        /// </summary>
        protected void EnsuresNotDisposed()
        {
            if (this.Disposed)
            {
                throw new ObjectDisposedException(this.GetType().Name);
            }
        }
    }
}
