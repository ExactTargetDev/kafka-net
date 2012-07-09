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

using Kafka.Client.Responses;

namespace Kafka.Client.Producers.Sync
{
    using System;
    using System.Collections.Generic;
    using Kafka.Client.Messages;
    using Kafka.Client.Requests;

    /// <summary>
    /// Sends messages encapsulated in request to Kafka server synchronously
    /// </summary>
    public interface ISyncProducer : IDisposable
    {
        /// <summary>
        /// Sends a producer request to Kafka server synchronously
        /// </summary>
        /// <param name="request">
        /// The request.
        /// </param>
        ProducerResponse Send(ProducerRequest request);

        /// <summary>
        /// Sends a topic metadata request to Kafka server synchronously
        /// </summary>
        /// <param name="request">The Request</param>
        /// <returns>The Response</returns>
        IEnumerable<TopicMetadata> Send(TopicMetadataRequest request);
    }
}
