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
using Kafka.Client.Requests;
using Kafka.Client.Serialization;
using Kafka.Client.Utils;

namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class OffsetDetail : IWritable
    {
        public const byte DefaultTopicSizeSize = 2;
        public const byte DefaultPartitionsSizeSize = 4;
        public const byte DefaultOffsetsSizeSize = 4;
        public const byte DefaultFetchSizesSizeSize = 4;

        public const byte DefaultPartitionSize = 4;
        public const byte DefaultOffsetSize = 8;
        public const byte DefaultFetchSizeSize = 4;

        public string Topic { get; private set; }

        public IEnumerable<int> Partitions { get; private set; }

        public IEnumerable<long> Offsets { get; private set; }

        public IEnumerable<int> FetchSizes { get; private set; }

        public OffsetDetail(string topic, IEnumerable<int> partitions, IEnumerable<long> offsets, IEnumerable<int> fetchSizes)
        {
            this.Topic = topic;
            this.Partitions = partitions;
            this.Offsets = offsets;
            this.FetchSizes = fetchSizes;
        }

        public int SizeInBytes
        {
            get
            {
                return DefaultTopicSizeSize +
                       this.Topic.Length +
                       DefaultPartitionsSizeSize + this.Partitions.Sum(x => DefaultPartitionSize) +
                       DefaultOffsetsSizeSize + this.Offsets.Sum(x => DefaultOffsetSize) +
                       DefaultFetchSizesSizeSize + this.FetchSizes.Sum(x => DefaultFetchSizeSize);
            }
        }

        public void WriteTo(MemoryStream output)
        {
            Guard.NotNull(output, "output");

            using (var writer = new KafkaBinaryWriter(output))
            {
                this.WriteTo(writer);
            }
        }

        public void WriteTo(KafkaBinaryWriter writer)
        {
            Guard.NotNull(writer, "writer");

            writer.WriteShortString(this.Topic, AbstractRequest.DefaultEncoding);
            writer.Write(this.Partitions.Count());
            foreach (var partition in this.Partitions)
            {
                writer.Write(partition);
            }

            writer.Write(this.Offsets.Count());
            foreach (var offset in this.Offsets)
            {
                writer.Write(offset);
            }

            writer.Write(this.FetchSizes.Count());
            foreach (var fetchSize in this.FetchSizes)
            {
                writer.Write(fetchSize);
            }
        }

        internal static OffsetDetail ParseFrom(KafkaBinaryReader reader)
        {
            var topic = BitWorks.ReadShortString(reader, AbstractRequest.DefaultEncoding);
            var partitionsCount = reader.ReadInt32();
            var partitions = new int[partitionsCount];
            for (int i = 0; i < partitionsCount; i++)
            {
                partitions[i] = reader.ReadInt32();
            }
            var offsetsCount = reader.ReadInt32();
            var offsets = new long[offsetsCount];
            for (int i = 0; i < offsetsCount; i++)
            {
                offsets[i] = reader.ReadInt64();
            }
            var fetchSizesCount = reader.ReadInt32();
            var fetchSizes = new int[fetchSizesCount];
            for (int i = 0; i < fetchSizesCount; i++)
            {
                fetchSizes[i] = reader.ReadInt32();
            }
            return new OffsetDetail(topic, partitions, offsets, fetchSizes);
        }
    }
}
