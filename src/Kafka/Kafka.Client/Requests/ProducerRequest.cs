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

namespace Kafka.Client.Requests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using System.Linq;
    using Kafka.Client.Messages;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using Kafka.Client.Producers;

    /// <summary>
    /// Constructs a request to send to Kafka.
    /// </summary>
    public class ProducerRequest : AbstractRequest, IWritable
    {
        public const int RandomPartition = -1;
        public const short CurrentVersion = 0;
        public const byte DefaultTopicSizeSize = 2;
        public const byte DefaultPartitionSize = 4;
        public const byte DefaultSetSizeSize = 4;
        public const byte DefaultHeaderSize = DefaultRequestSizeSize + DefaultTopicSizeSize + DefaultPartitionSize + DefaultRequestIdSize + DefaultSetSizeSize;

        public const byte VersionIdSize = 2;
        public const byte CorrelationIdSize = 4;
        public const byte ClientIdSize = 2;
        public const byte RequiredAcksSize = 2;
        public const byte AckTimeoutSize = 4;
        public const byte NumberOfTopicsSize = 4;
        public const byte DefaultHeaderSize8 = DefaultRequestSizeSize + DefaultRequestIdSize + VersionIdSize + CorrelationIdSize + ClientIdSize + RequiredAcksSize + AckTimeoutSize + NumberOfTopicsSize;

        public short VersionId { get; set; }
        public int CorrelationId { get; set; }
        public string ClientId { get; set; }
        public short RequiredAcks { get; set; }
        public int AckTimeout { get; set; }
        public IEnumerable<TopicData> Data { get; set; }

        public int GetRequestLength()
        {
            return DefaultHeaderSize8 + GetShortStringLength(this.ClientId) + this.Data.Sum(item => item.SizeInBytes);
        }

        protected short GetShortStringLength(string text, string encoding = AbstractRequest.DefaultEncoding)
        {
            if (string.IsNullOrEmpty(text))
            {
                return (short)0;
            }
            else
            {
                Encoding encoder = Encoding.GetEncoding(encoding);
                return (short) encoder.GetByteCount(text);
            }
        }

        public ProducerRequest(short versionId, int correlationId, string clientId, short requiredAcks, int ackTimeout, IEnumerable<TopicData> data)
        {
            this.VersionId = versionId;
            this.CorrelationId = correlationId;
            this.ClientId = clientId;
            this.RequiredAcks = requiredAcks;
            this.AckTimeout = ackTimeout;
            this.Data = data;
            int length = GetRequestLength();
            this.RequestBuffer = new BoundedBuffer(length);
            this.WriteTo(this.RequestBuffer);
        }

        public ProducerRequest(int correlationId, string clientId, short requiredAcks, int ackTimeout, IEnumerable<TopicData> data)
            : this(CurrentVersion, correlationId, clientId, requiredAcks, ackTimeout, data)
        { 
        }

        [Obsolete]
        public ProducerRequest(string topic, int partition, BufferedMessageSet messages)
        {
            Guard.NotNull(messages, "messages");
            this.Topic = topic;
            this.Partition = partition;
            this.MessageSet = messages;
            int length = GetRequestLength();
            this.RequestBuffer = new BoundedBuffer(length);
            this.WriteTo(this.RequestBuffer);
        }

        /// <summary>
        /// Initializes a new instance of the ProducerRequest class.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="messages">The list of messages to send.</param>
        [Obsolete]
        public ProducerRequest(string topic, int partition, IEnumerable<Message> messages)
            : this(topic, partition, new BufferedMessageSet(messages))
        {
        }

        public BufferedMessageSet MessageSet { get; private set; }

        public override RequestTypes RequestType
        {
            get
            {
                return RequestTypes.Produce;
            }
        }

        public int TotalSize
        {
            get
            {
                return (int)this.RequestBuffer.Length;
            }
        }

        /// <summary>
        /// Writes content into given stream
        /// </summary>
        /// <param name="output">
        /// The output stream.
        /// </param>
        public void WriteTo(MemoryStream output)
        {
            Guard.NotNull(output, "output");

            using (var writer = new KafkaBinaryWriter(output))
            {
                writer.Write(this.RequestBuffer.Capacity - DefaultRequestSizeSize);
                writer.Write(this.RequestTypeId);
                this.WriteTo(writer);
            }
        }

        /// <summary>
        /// Writes content into given writer
        /// </summary>
        /// <param name="writer">
        /// The writer.
        /// </param>
        public void WriteTo(KafkaBinaryWriter writer)
        {
            Guard.NotNull(writer, "writer");

            writer.Write(this.VersionId);
            writer.Write(this.CorrelationId);
            writer.WriteShortString(this.ClientId, DefaultEncoding);
            writer.Write(this.RequiredAcks);
            writer.Write(this.AckTimeout);
            writer.Write(this.Data.Count());
            foreach (var topicData in this.Data)
            {
                writer.WriteShortString(topicData.Topic, DefaultEncoding);
                writer.Write(topicData.PartitionData.Count());
                foreach (var partitionData in topicData.PartitionData)
                {
                    writer.Write(partitionData.Partition);
                    writer.Write(partitionData.Messages.SetSize);
                    partitionData.Messages.WriteTo(writer);
                }
            }

            //writer.WriteShortString(this.Topic, DefaultEncoding);
            //writer.Write(this.Partition);
            //writer.Write(this.MessageSet.SetSize);
            //this.MessageSet.WriteTo(writer);
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("Request size: ");
            sb.Append(this.TotalSize);
            sb.Append(", RequestId: ");
            sb.Append(this.RequestTypeId);
            sb.Append("(");
            sb.Append((RequestTypes)this.RequestTypeId);
            sb.Append(")");
            sb.Append(", Topic: ");
            sb.Append(this.Topic);
            sb.Append(", Partition: ");
            sb.Append(this.Partition);
            sb.Append(", Set size: ");
            sb.Append(this.MessageSet.SetSize);
            sb.Append(", Set {");
            sb.Append(this.MessageSet.ToString());
            sb.Append("}");
            return sb.ToString();
        }
    }
}
