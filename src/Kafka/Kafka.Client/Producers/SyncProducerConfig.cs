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

namespace Kafka.Client.Producers
{
    using Kafka.Client.Cfg.Sections;
    using Kafka.Client.Common;

    public class SyncProducerConfig : Config
    {
        public const string DefaultClientId = "";

        public const short DefaultRequiredAcks = 0;

        public const int DefaultAckTimeout = 10000;

        public const int DefaultSendBufferBytes = 100 * 1024;

        public SyncProducerConfig()
        {
            this.SendBufferBytes = DefaultSendBufferBytes;
            this.ClientId = DefaultClientId;
            this.RequestRequiredAcks = DefaultRequiredAcks;
            this.RequestTimeoutMs = DefaultAckTimeout;
        }

        public SyncProducerConfig(ProducerConfig config, string host, int port) 
        {
            this.Host = host;
            this.Port = port;
            this.SendBufferBytes = config.SendBufferBytes;
            this.ClientId = config.ClientId;
            this.RequestRequiredAcks = config.RequestRequiredAcks;
            this.RequestTimeoutMs = config.RequestTimeoutMs;
        }

        public SyncProducerConfig(ProducerConfigurationSection config, string host, int port)
        {
            this.Host = host;
            this.Port = port;
            this.SendBufferBytes = config.SendBufferBytes;
            this.ClientId = config.ClientId;
            this.RequestRequiredAcks = config.RequestRequiredAcks;
            this.RequestTimeoutMs = config.RequestTimeoutMs;
        }

        public string Host { get; set; }

        public int Port { get; set; }

        public int SendBufferBytes { get; set; }

        /// <summary>
        /// the client application sending the producer requests
        /// </summary>
        public string ClientId { get; set; }

        /// <summary>
        /// The required acks of the producer requests - negative value means ack
        /// after the replicas in ISR have caught up to the leader's offset
        /// corresponding to this produce request.
        /// </summary>
        public short RequestRequiredAcks { get; set; }

        /// <summary>
        /// The ack timeout of the producer requests. Value must be non-negative and non-zero
        /// </summary>
        public int RequestTimeoutMs { get; set; }
    }
}
