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

namespace Kafka.Client.Cfg
{
    using System.Collections.Generic;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;

    /// <summary>
    /// Configuration used by the asynchronous producer
    /// </summary>
    public class AsyncProducerConfiguration : SyncProducerConfiguration, IAsyncProducerConfigShared
    {
        public const string DefaultSerializerClass = "Kafka.Client.Serialization.DefaultEncoder";
        public const int DefaultQueueTime = 5000;
        public const int DefaultQueueSize = 10000;
        public const int DefaultBatchSize = 200;

        public AsyncProducerConfiguration()
        {
            this.SerializerClass = DefaultSerializerClass;
            this.QueueTime = DefaultQueueTime;
            this.QueueSize = DefaultQueueSize;
            this.BatchSize = DefaultBatchSize;
        }

        public AsyncProducerConfiguration(ProducerConfiguration config, int id, string host, int port) 
            : base(config, id, host, port)
        {
            Guard.NotNull(config, "config");
            this.SerializerClass = config.SerializerClass;
            this.QueueTime = config.QueueTime;
            this.QueueSize = config.QueueSize;
            this.BatchSize = config.BatchSize;
        }

        public string SerializerClass { get; set; }

        public string CallbackHandlerClass { get; set; }

        public int QueueTime { get; set; }

        public int QueueSize { get; set; }

        public int BatchSize { get; set; }
    }
}
