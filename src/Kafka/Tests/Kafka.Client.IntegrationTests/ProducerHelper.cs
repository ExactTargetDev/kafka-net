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

namespace Kafka.Client.IntegrationTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Kafka.Client.Cfg;
    using Kafka.Client.Requests;
    using Kafka.Client.Producers;
    using Kafka.Client.Producers.Sync;
    using Kafka.Client.Messages;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class ProducerHelper : IntegrationFixtureBase
    {
        public Tuple<ProducerResponse,PartitionMetadata> SendMessagesToTopicSynchronously(string topic, IEnumerable<Message> messages, SyncProducerConfiguration syncProducerConfig)
        {
            var prodConfig = syncProducerConfig;

            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(new List<string>() { topic });
            IEnumerable<TopicMetadata> topicMetadata = null;

            using (var producer = new SyncProducer(prodConfig))
            {
                topicMetadata = producer.Send(topicMetadataRequest);
            }

            var topicMetadataItem = topicMetadata.ToArray()[0];
            var partitionMetadata = topicMetadataItem.PartitionsMetadata.ToArray()[0];
            var broker = partitionMetadata.Replicas.ToArray()[0];
            prodConfig.BrokerId = broker.Id;
            prodConfig.Host = broker.Host;
            prodConfig.Port = broker.Port;

            using (var producer = new SyncProducer(prodConfig))
            {
                var bufferedMessageSet = new BufferedMessageSet(new List<Message>(messages));

                var req = new ProducerRequest(-1, "", 0, 0,
                                              new List<TopicData>()
                                                  {
                                                      new TopicData(topic,
                                                                    new List<PartitionData>() {new PartitionData(partitionMetadata.PartitionId, bufferedMessageSet)})
                                                  });
                var producerResponse = producer.Send(req);
                return new Tuple<ProducerResponse, PartitionMetadata>(producerResponse, partitionMetadata);

            }
        }
    }
}
