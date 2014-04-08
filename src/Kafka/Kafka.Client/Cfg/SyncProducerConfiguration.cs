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
    using Kafka.Client.Utils;

    public class SyncProducerConfiguration : ISyncProducerConfigShared
    {
        public const string DefaultClientId = "";

        public const short DefaultRequiredAcks = 0;

        public const int DefaultAckTimeout = 10000;

        public const int DefaultSendBufferBytes = 100 * 1024;

        public SyncProducerConfiguration()
        {
            this.SendBufferBytes = DefaultSendBufferBytes;
            this.ClientId = DefaultClientId;
            this.RequestRequiredAcks = DefaultRequiredAcks;
            this.RequestTimeoutMs = DefaultAckTimeout;
        }

        public SyncProducerConfiguration(ProducerConfig config, string host, int port) 
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

        public string ClientId { get; set; }

        public short RequestRequiredAcks { get; set; }

        public int RequestTimeoutMs { get; set; }
       
    }
}
