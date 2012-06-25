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

using System.IO;
using Kafka.Client.Exceptions;
using Kafka.Client.Producers;

namespace Kafka.Client.Requests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using Kafka.Client.Messages;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class TopicMetadataRequest : AbstractRequest, IWritable
    {
        public const int DefaultNumberOfTopicsSize = 4;
        public const int DefaultDetailedMetadataSize = 2;
        public const int DefaultTimestampSize = 8;
        public const int DefaultCountSize = 4;
        public const byte DefaultHeaderSize8 = DefaultRequestSizeSize + DefaultRequestIdSize;

        public IEnumerable<string> Topics { get; private set; }

        public short DetailedMetadata { get; private set; }

        public long? Timestamp { get; private set; }

        public int? Count { get; private set; }

        public TopicMetadataRequest(IEnumerable<string> topics, short detailedMetadata = DetailedMetadataRequest.NoSegmentMetadata, long? timestamp = null, int? count = null)
        {
            this.Topics = topics;
            this.DetailedMetadata = detailedMetadata;
            this.Timestamp = timestamp;
            this.Count = count;
            int length = GetRequestLength();
            this.RequestBuffer = new BoundedBuffer(length);
            this.WriteTo(this.RequestBuffer);
        }

        public override RequestTypes RequestType
        {
            get { return RequestTypes.TopicMetadataRequest; }
        }

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

        public void WriteTo(KafkaBinaryWriter writer)
        {
            Guard.NotNull(writer, "writer");

            writer.Write(this.Topics.Count());
            foreach (var topic in Topics)
            {
                writer.WriteShortString(topic, AbstractRequest.DefaultEncoding);
            }
            writer.Write(this.DetailedMetadata);
            if (this.DetailedMetadata == DetailedMetadataRequest.SegmentMetadata)
            {
                writer.Write(this.Timestamp ?? 0);
                writer.Write(this.Count ?? 0);
            }
        }

        public int GetRequestLength()
        {
            var size = DefaultHeaderSize8 + DefaultNumberOfTopicsSize + this.Topics.Sum(x => BitWorks.GetShortStringLength(x, AbstractRequest.DefaultEncoding)) + DefaultDetailedMetadataSize;
            if (this.DetailedMetadata == DetailedMetadataRequest.SegmentMetadata)
            {
                size += DefaultTimestampSize + DefaultCountSize;
            }
            return size;
        }

        public static IEnumerable<TopicMetadata> DeserializeTopicsMetadataResponse(KafkaBinaryReader reader)
        {
            //should I do anything withi this:
            int length = reader.ReadInt32();

            short errorCode = reader.ReadInt16();
            if (errorCode != KafkaException.NoError)
            {
                //ignore the error
            }

            var numTopics = reader.ReadInt32();
            var topicMetadata = new TopicMetadata[numTopics];
            for (int i = 0; i < numTopics; i++)
            {
                topicMetadata[i] = TopicMetadata.ParseFrom(reader);
            }
            return topicMetadata;
        }
    }

    public static class DetailedMetadataRequest
    {
        public const short SegmentMetadata = (short)1;

        public const short NoSegmentMetadata = (short)0;
    }
}
