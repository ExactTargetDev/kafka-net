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

using Kafka.Client.Consumers;

namespace Kafka.Client.Requests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class FetchRequestBuilder
    {
        private int correlationId = -1;
        private short versionId = FetchRequest.CurrentVersion;
        private string clientId = string.Empty;
        private int replicaId = -1;
        private int maxWait = -1;
        private int minBytes = -1;
        private Dictionary<string, Tuple<List<int>, List<long>, List<int>>> requestMap = new Dictionary<string, Tuple<List<int>, List<long>, List<int>>>();

        public FetchRequestBuilder AddFetch(string topic, int partition, long offset, int fetchSize)
        {
            if (!this.requestMap.ContainsKey(topic))
            {
                var item = new Tuple<List<int>, List<long>, List<int>>(new List<int>(), new List<long>(),
                                                                       new List<int>());
                this.requestMap.Add(topic, item);
            }
            this.requestMap[topic].Item1.Add(partition);
            this.requestMap[topic].Item2.Add(offset);
            this.requestMap[topic].Item3.Add(fetchSize);
            return this;
        }

        public FetchRequestBuilder CorrelationId(int correlationId)
        {
            this.correlationId = correlationId;
            return this;
        }

        public FetchRequestBuilder ClientId(string clientId)
        {
            this.clientId = clientId;
            return this;
        }

        public FetchRequestBuilder ReplicaId(int replicaId)
        {
            this.replicaId = replicaId;
            return this;
        }

        public FetchRequestBuilder MaxWait(int maxWait)
        {
            this.maxWait = maxWait;
            return this;
        }

        public FetchRequestBuilder MinBytes(int minBytes)
        {
            this.minBytes = minBytes;
            return this;
        }

        public FetchRequest Build()
        {
            var offsetDetails =
                this.requestMap.Select(
                    topicData =>
                    new OffsetDetail(topicData.Key, topicData.Value.Item1, topicData.Value.Item2, topicData.Value.Item3));
            return new FetchRequest(versionId, correlationId, clientId, replicaId, maxWait, minBytes, offsetDetails);
        }
    }
}
