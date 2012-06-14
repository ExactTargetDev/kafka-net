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

namespace Kafka.Client.Producers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Messages;
    using Kafka.Client.Utils;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class PartitionData
    {
        public const byte DefaultPartitionNrSize = 4;
        public const byte DefaultMessagesSizeSize = 4;

        public PartitionData(int partition, int error, MessageSet messages)
        {
            this.Partition = partition;
            this.Messages = messages;
            this.Error = error;
        }

        public PartitionData(int partition, MessageSet messages)
            : this(partition, ErrorMapping.NoError, messages)
        {
        }

        public int Partition { get; private set; }

        public MessageSet Messages { get; private set; }

        public int Error { get; private set; }

        public int SizeInBytes
        {
            get { return DefaultPartitionNrSize + DefaultMessagesSizeSize + this.Messages.SetSize; }
        }
    }
}
