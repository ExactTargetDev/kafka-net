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
using log4net;

namespace Kafka.Client.Producers.Sync
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Kafka.Client.Cfg;
    using Kafka.Client.Messages;
    using Kafka.Client.Requests;
    using Kafka.Client.Utils;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Responses;

    /// <summary>
    /// Sends messages encapsulated in request to Kafka server synchronously
    /// </summary>
    public class SyncProducer : ISyncProducer
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private readonly KafkaConnection connection;
        private static object SendLock = new object();

        private volatile bool disposed;

        /// <summary>
        /// Gets producer config
        /// </summary>
        public SyncProducerConfiguration Config { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncProducer"/> class.
        /// </summary>
        /// <param name="config">
        /// The producer config.
        /// </param>
        public SyncProducer(SyncProducerConfiguration config)
        {
            Guard.NotNull(config, "config");
            this.Config = config;
            this.connection = new KafkaConnection(
                this.Config.Host, 
                this.Config.Port,
                config.BufferSize,
                config.SocketTimeout);
        }

        /// <summary>
        /// Sends request to Kafka server synchronously
        /// </summary>
        /// <param name="request">
        /// The request.
        /// </param>
        public ProducerResponse Send(ProducerRequest request)
        {
            this.EnsuresNotDisposed();
            foreach (var topicData in request.Data)
            {
                foreach (var partitionData in topicData.PartitionData)
                {
                    VerifyMessageSize(partitionData.Messages.Messages);
                }
            }
            lock (SendLock)
            {
                this.connection.Write(request);
                return ProducerResponse.ParseFrom(this.connection.Reader);
            }
        }

        //public ProducerResponse Send(string topic, BufferedMessageSet messages)
        //{
        //    var partitionData = new PartitionData[] { new PartitionData(ProducerRequest.RandomPartition, messages) };
        //    var data = new TopicData[] {new TopicData(topic, partitionData)};
        //    var producerRequest = new ProducerRequest(ProducerRequest.RandomPartition, string.Empty, 0, 0, data);
        //    return this.Send(producerRequest);
        //}

        public IEnumerable<TopicMetadata> Send(TopicMetadataRequest request)
        {
            lock (SendLock)
            {
                this.connection.Write(request);
                return TopicMetadataRequest.DeserializeTopicsMetadataResponse(this.connection.Reader);
            }
        }

        /// <summary>
        /// Releases all unmanaged and managed resources
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing)
            {
                return;
            }

            if (this.disposed)
            {
                return;
            }

            this.disposed = true;
            if (this.connection != null)
            {
                this.connection.Dispose();
            }
        }

        /// <summary>
        /// Ensures that object was not disposed
        /// </summary>
        private void EnsuresNotDisposed()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException(this.GetType().Name);
            }
        }

        private void VerifyMessageSize(IEnumerable<Message> messages)
        {
            foreach (var message in messages)
            {
                if (message.PayloadSize > Config.MaxMessageSize)
                {
                    throw new MessageSizeTooLargeException();
                }
            }
        }
    }
}
