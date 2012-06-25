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

using Kafka.Client.Exceptions;
using Kafka.Client.Messages;
using Kafka.Client.Producers;
using Kafka.Client.Serialization;

namespace Kafka.Client.Responses
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class FetchResponse
    {
        public short VersionId { get; set; }
        public int CorrelationId { get; set; }
        public IEnumerable<TopicData> Data { get; set; }

        public FetchResponse(short versionId, int correlationId, IEnumerable<TopicData> data)
        {
            this.VersionId = versionId;
            this.CorrelationId = correlationId;
            this.Data = data;
        }

        public BufferedMessageSet MessageSet(string topic, int partition)
        {
            var topicMap = this.Data.GroupBy(x => x.Topic, x => x)
                .ToDictionary(x => x.Key, x => x.ToList().FirstOrDefault());

            var messageSet = new BufferedMessageSet(Enumerable.Empty<Message>());
            if (topicMap.ContainsKey(topic))
            {
                var topicData = topicMap[topic];
                if (topicData != null)
                {
                    var messages = TopicData.FindPartition(topicData.PartitionData, partition).Messages;
                    messageSet = new BufferedMessageSet(messages.Messages);
                }
            }
            return messageSet;
        }

        public static FetchResponse ParseFrom(KafkaBinaryReader reader)
        {
            //should I do anything withi this:
            int length = reader.ReadInt32();

            short errorCode = reader.ReadInt16();
            if (errorCode != KafkaException.NoError)
            {
                //ignore the error
            }

            var versionId = reader.ReadInt16();
            var correlationId = reader.ReadInt32();
            var dataCount = reader.ReadInt32();
            var data = new TopicData[dataCount];
            for (int i = 0; i < dataCount; i++)
            {
                data[i] = TopicData.ParseFrom(reader);
            }
            return new FetchResponse(versionId, correlationId, data);
        }
    }
}
