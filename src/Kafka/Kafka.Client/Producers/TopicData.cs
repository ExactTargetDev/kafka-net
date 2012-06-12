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

using Kafka.Client.Requests;

namespace Kafka.Client.Producers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class TopicData
    {
        public const byte DefaultNumberOfPartitionsSize = 4;
        public const byte DefaultTopicSizeSize = 2;

        public TopicData(string topic, IEnumerable<PartitionData> partitionData)
        {
            this.Topic = topic;
            this.PartitionData = partitionData;
        }

        public string Topic { get; private set; }

        public IEnumerable<PartitionData> PartitionData { get; private set; }

        public int SizeInBytes
        {
            get
            {
                var topicLength = GetTopicLength(this.Topic);
                return DefaultTopicSizeSize + topicLength + DefaultNumberOfPartitionsSize + this.PartitionData.Sum(dataPiece => dataPiece.SizeInBytes);
            }
        }

        protected static short GetTopicLength(string topic, string encoding = AbstractRequest.DefaultEncoding)
        {
            Encoding encoder = Encoding.GetEncoding(encoding);
            return string.IsNullOrEmpty(topic) ? AbstractRequest.DefaultTopicLengthIfNonePresent : (short)encoder.GetByteCount(topic);
        }
    }
}
