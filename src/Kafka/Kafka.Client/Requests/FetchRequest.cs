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

using System.Collections.Generic;
using Kafka.Client.Consumers;

namespace Kafka.Client.Requests
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Text;
    using Kafka.Client.Messages;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using System.Linq;

    /// <summary>
    /// Constructs a request to send to Kafka.
    /// </summary>
    public class FetchRequest : AbstractRequest, IWritable
    {
        /// <summary>
        /// Maximum size.
        /// </summary>
        private static readonly int DefaultMaxSize = 1048576;
        public const byte DefaultTopicSizeSize = 2;
        public const byte DefaultPartitionSize = 4;
        public const byte DefaultOffsetSize = 8;
        public const byte DefaultMaxSizeSize = 4;
        public const byte DefaultHeaderSize = DefaultRequestSizeSize + DefaultTopicSizeSize + DefaultPartitionSize + DefaultRequestIdSize + DefaultOffsetSize + DefaultMaxSizeSize;
        public const byte DefaultHeaderAsPartOfMultirequestSize = DefaultTopicSizeSize + DefaultPartitionSize + DefaultOffsetSize + DefaultMaxSizeSize;

        public const byte DefaultVersionIdSize = 2;
        public const byte DefaultCorrelationIdSize = 4;
        public const byte DefaultReplicaIdSize = 4;
        public const byte DefaultMaxWaitSize = 4;
        public const byte DefaultMinBytesSize = 4;
        public const byte DefaultOffsetInfoSizeSize = 4;

        public const short CurrentVersion = (short)1;

        public short VersionId { get; set; }
        public int CorrelationId { get; set; }
        public string ClientId { get; set; }
        public int ReplicaId { get; set; }
        public int MaxWait { get; set; }
        public int MinBytes { get; set; }
        public IEnumerable<OffsetDetail> OffsetInfo { get; set; }

        public int GetRequestLength()
        {
            return DefaultRequestSizeSize + 
                   DefaultRequestIdSize +
                   DefaultVersionIdSize +
                   DefaultCorrelationIdSize +
                   BitWorks.GetShortStringLength(this.ClientId, AbstractRequest.DefaultEncoding) +
                   DefaultReplicaIdSize +
                   DefaultMaxWaitSize +
                   DefaultMinBytesSize +
                   DefaultOffsetInfoSizeSize + this.OffsetInfo.Sum(x => x.SizeInBytes);
        }

        [Obsolete]
        public static int GetRequestAsPartOfMultirequestLength(string topic, string encoding = DefaultEncoding)
        {
            short topicLength = GetTopicLength(topic, encoding);
            return topicLength + DefaultHeaderAsPartOfMultirequestSize;
        }

        public FetchRequest(int correlationId, string clientId, int replicaId, int maxWait, int minBytes,
                            IEnumerable<OffsetDetail> offsetInfo)
            : this(CurrentVersion, correlationId, clientId, replicaId, maxWait, minBytes, offsetInfo)
        {
        }

        public FetchRequest(short versionId, int correlationId, string clientId, int replicaId, int maxWait, int minBytes, IEnumerable<OffsetDetail> offsetInfo)
        {
            this.VersionId = versionId;
            this.CorrelationId = correlationId;
            this.ClientId = clientId;
            this.ReplicaId = replicaId;
            this.MaxWait = maxWait;
            this.MinBytes = minBytes;
            this.OffsetInfo = offsetInfo;

            int length = GetRequestLength();
            this.RequestBuffer = new BoundedBuffer(length);
            this.WriteTo(this.RequestBuffer);
        }

        /// <summary>
        /// Initializes a new instance of the FetchRequest class.
        /// </summary>
        [Obsolete]
        public FetchRequest()
        {
        }

        /// <summary>
        /// Initializes a new instance of the FetchRequest class.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="offset">The offset in the topic/partition to retrieve from.</param>
        [Obsolete]
        public FetchRequest(string topic, int partition, long offset)
            : this(topic, partition, offset, DefaultMaxSize)
        {
        }

        /// <summary>
        /// Initializes a new instance of the FetchRequest class.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="offset">The offset in the topic/partition to retrieve from.</param>
        /// <param name="maxSize">The maximum size.</param>
        [Obsolete]
        public FetchRequest(string topic, int partition, long offset, int maxSize)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
            MaxSize = maxSize;

            int length = this.GetRequestLength();
            this.RequestBuffer = new BoundedBuffer(length);
            this.WriteTo(this.RequestBuffer);
        }

        /// <summary>
        /// Gets or sets the offset to request.
        /// </summary>
        public long Offset { get; set; }

        /// <summary>
        /// Gets or sets the maximum size to pass in the request.
        /// </summary>
        public int MaxSize { get; set; }

        public override RequestTypes RequestType
        {
            get
            {
                return RequestTypes.Fetch;
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
            writer.WriteShortString(this.ClientId, AbstractRequest.DefaultEncoding);
            writer.Write(this.ReplicaId);
            writer.Write(this.MaxWait);
            writer.Write(this.MinBytes);
            writer.Write(this.OffsetInfo.Count());
            foreach (var offsetInfo in this.OffsetInfo)
            {
                offsetInfo.WriteTo(writer);
            }
        }

        public override string ToString()
        {
            return String.Format(
                CultureInfo.CurrentCulture,
                "varsionId: {0}, correlationId: {1}, clientId: {2}, replicaId: {3}, maxWait: {4}, minBytes: {5}",
                this.VersionId,
                this.CorrelationId,
                this.ClientId,
                this.ReplicaId,
                this.MaxWait,
                this.MinBytes);
        }
    }
}
