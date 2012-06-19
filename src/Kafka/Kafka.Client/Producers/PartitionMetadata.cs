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

using Kafka.Client.Cluster;
using Kafka.Client.Serialization;
using Kafka.Client.Utils;

namespace Kafka.Client.Producers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class PartitionMetadata : IWritable
    {
        public const byte DefaultPartitionIdSize = 4;
        public const byte DefaultIfLeaderExistsSize = 1;
        public const byte DefaultNumberOfReplicasSize = 2;
        public const byte DefaultNumberOfSyncReplicasSize = 2;
        public const byte DefaultIfLogSegmentMetadataExistsSize = 1;

        public int PartitionId { get; private set; }

        internal Broker Leader { get; private set; }

        internal IEnumerable<Broker> Replicas { get; private set; }

        internal IEnumerable<Broker> Isr { get; private set; }

        public LogMetadata LogMetadata { get; private set; }

        internal PartitionMetadata(int partitionId, Broker leader, IEnumerable<Broker> replicas, IEnumerable<Broker> isr, LogMetadata logMetadata = null)
        {
            this.PartitionId = partitionId;
            this.Leader = leader;
            this.Replicas = replicas;
            this.Isr = isr;
            this.LogMetadata = logMetadata;
        }

        public int SizeInBytes
        {
            get
            {
                var size = DefaultPartitionIdSize + DefaultIfLeaderExistsSize;
                if (this.Leader != null)
                {
                    size += this.Leader.SizeInBytes;
                }
                size += DefaultNumberOfReplicasSize;
                foreach (var replica in Replicas)
                {
                    size += replica.SizeInBytes;
                }
                size += DefaultNumberOfSyncReplicasSize;
                foreach (var isr in Isr)
                {
                    size += isr.SizeInBytes;
                }
                size += DefaultIfLogSegmentMetadataExistsSize;
                if (this.LogMetadata != null)
                {
                    size += this.LogMetadata.SizeInBytes;
                }
                return size;
            }
        }

        public void WriteTo(System.IO.MemoryStream output)
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

            // if leader exists
            writer.Write(this.PartitionId);
            if (this.Leader != null)
            {
                writer.Write((byte)1);
                this.Leader.WriteTo(writer);
            }
            else
            {
                writer.Write((byte)0);
            }

            // number of replicas
            writer.Write((short)this.Replicas.Count());
            foreach (var replica in Replicas)
            {
                replica.WriteTo(writer);
            }

            // number of in-sync replicas
            writer.Write((short)this.Isr.Count());
            foreach (var isr in Isr)
            {
                isr.WriteTo(writer);
            }

            // if log segment metadata exists
            if (this.LogMetadata != null)
            {
                writer.Write((byte)1);
                this.LogMetadata.WriteTo(writer);
            }
            else
            {
                writer.Write((byte)0);
            }
        }

        public static PartitionMetadata ParseFrom(KafkaBinaryReader reader)
        {
            var partitionId = reader.ReadInt32();
            var doesLeaderExist = reader.ReadByte();
            Broker leader = null;
            if (doesLeaderExist == (byte)1)
            {
                leader = Broker.ParseFrom(reader);
            }

            // list of all replicas
            var numReplicas = reader.ReadInt16();
            var replicas = new Broker[numReplicas];
            for (int i = 0; i < numReplicas; i++)
            {
                replicas[i] = Broker.ParseFrom(reader);
            }

            // list of in-sync replicas
            var numIsr = reader.ReadInt16();
            var isr = new Broker[numIsr];
            for (int i = 0; i < numIsr; i++)
            {
                replicas[i] = Broker.ParseFrom(reader);
            }

            var doesLogMetadataExist = reader.ReadByte();
            LogMetadata logMetadata = null;
            if (doesLogMetadataExist == (byte)1)
            {
                var numLogSegments = reader.ReadInt32();
                var totalDataSize = reader.ReadInt64();
                var numSegmentMetadata = reader.ReadInt32();
                IEnumerable<LogSegmentMetadata> segmentMetadata = null;
                if (numSegmentMetadata > 0)
                {
                    var result = new List<LogSegmentMetadata>();
                    for (int i = 0; i < numSegmentMetadata; i++)
                    {
                        var beginningOffset = reader.ReadInt64();
                        var lastModified = reader.ReadInt64();
                        var size = reader.ReadInt64();
                        result.Add(new LogSegmentMetadata(beginningOffset, lastModified, size));
                    }
                    segmentMetadata = result;
                }
                logMetadata = new LogMetadata(numLogSegments, totalDataSize, segmentMetadata);
            }
            return new PartitionMetadata(partitionId, leader, replicas, isr, logMetadata);
        }
    }
}
