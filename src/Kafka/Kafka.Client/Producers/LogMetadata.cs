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
    public class LogMetadata : IWritable
    {
        public const byte DefaultNumLogSegmentsSize = 4;
        public const byte DefaultTotalSizeSize = 8;

        public int NumLogSegments { get; private set; }

        public long TotalSize { get; private set; }

        public IEnumerable<LogSegmentMetadata> LogSegmentMetadata { get; private set; }

        public LogMetadata(int numLogSegment, long totalSize, IEnumerable<LogSegmentMetadata> logSegmentMetadata)
        {
            this.NumLogSegments = numLogSegment;
            this.TotalSize = totalSize;
            this.LogSegmentMetadata = logSegmentMetadata;
        }

        public int SizeInBytes
        {
            get
            {
                var size = DefaultNumLogSegmentsSize + DefaultTotalSizeSize;
                if (this.LogSegmentMetadata != null)
                {
                    size += this.LogSegmentMetadata.Sum(x => x.SizeInBytes);
                }
                return size;
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

            writer.Write(this.NumLogSegments);
            writer.Write(this.TotalSize);
            if (this.LogSegmentMetadata != null)
            {
                writer.Write(this.LogSegmentMetadata.Count());
                foreach (var logSegmentMetadata in LogSegmentMetadata)
                {
                    logSegmentMetadata.WriteTo(writer);
                }
            }
            else
            {
                writer.Write(0);
            }
        }
    }
}
