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
namespace Kafka.Client.Cfg.Sections
{
    using System.Configuration;
    using System.Xml.Linq;

    using Kafka.Client.Api;
    using Kafka.Client.Cfg.Elements;
    using Kafka.Client.Utils;

    public class ConsumerConfigurationSection : ConfigurationSection
    {
        [ConfigurationProperty("groupId", IsRequired = true)]
        public string GroupId
        {
            get
            {
                return (string)this["groupId"];
            }
        }

        [ConfigurationProperty("consumerId", IsRequired = false, DefaultValue = null)]
        public string ConsumerId
        {
            get
            {
                return (string)this["consumerId"];
            }
        }

        [ConfigurationProperty("socketTimeout", IsRequired = false, DefaultValue = ConsumerConfig.SocketTimeout)]
        public int SocketTimeout
        {
            get
            {
                return (int)this["socketTimeout"];
            }
        }

        [ConfigurationProperty("socketBufferSize", IsRequired = false, DefaultValue = ConsumerConfig.SocketBufferSize)]
        public int SocketBufferSize
        {
            get
            {
                return (int)this["socketBufferSize"];
            }
        }

        [ConfigurationProperty("fetchSize", IsRequired = false, DefaultValue = ConsumerConfig.FetchSize)]
        public int FetchSize
        {
            get
            {
                return (int)this["fetchSize"];
            }
        }

        [ConfigurationProperty("numConsumerFetchers", IsRequired = false, DefaultValue = ConsumerConfig.DefaultNumConsumerFetchers)]
        public int NumConsumerFetchers
        {
            get
            {
                return (int)this["numConsumerFetchers"];
            }
        }

        [ConfigurationProperty("autoCommit", IsRequired = false, DefaultValue = ConsumerConfig.AutoCommit)]
        public bool AutoCommit
        {
            get
            {
                return (bool)this["autoCommit"];
            }
        }

        [ConfigurationProperty("autoCommitInterval", IsRequired = false, DefaultValue = ConsumerConfig.AutoCommitInterval)]
        public int AutoCommitInterval
        {
            get
            {
                return (int)this["autoCommitInterval"];
            }
        }

        [ConfigurationProperty("maxQueuedChunks", IsRequired = false, DefaultValue = ConsumerConfig.MaxQueuedChunks)]
        public int MaxQueuedChunks
        {
            get
            {
                return (int)this["maxQueuedChunks"];
            }
        }

        [ConfigurationProperty("maxRebalanceRetries", IsRequired = false, DefaultValue = ConsumerConfig.MaxRebalanceRetries)]
        public int MaxRebalanceRetries
        {
            get
            {
                return (int)this["maxRebalanceRetries"];
            }
        }

        [ConfigurationProperty("minFetchBytes", IsRequired = false, DefaultValue = ConsumerConfig.MinFetchBytes)]
        public int MinFetchBytes
        {
            get
            {
                return (int)this["minFetchBytes"];
            }
        }

        [ConfigurationProperty("maxFetchWaitMs", IsRequired = false, DefaultValue = ConsumerConfig.MaxFetchWaitMs)]
        public int MaxFetchWaitMs
        {
            get
            {
                return (int)this["maxFetchWaitMs"];
            }
        }

        [ConfigurationProperty("rebalanceBackoffMs", IsRequired = false, DefaultValue = ZkConfig.DefaultSyncTime)]
        public int RebalanceBackoffMs
        {
            get
            {
                return (int)this["rebalanceBackoffMs"];
            }
        }

        [ConfigurationProperty("refreshMetadataBackoffMs", IsRequired = false, DefaultValue = ConsumerConfig.RefreshMetadataBackoffMs)]
        public int RefreshMetadataBackoffMs
        {
            get
            {
                return (int)this["refreshMetadataBackoffMs"];
            }
        }

        [ConfigurationProperty("autoOffsetReset", IsRequired = false, DefaultValue = OffsetRequest.LargestTimeString)]
        public string AutoOffsetReset
        {
            get
            {
                return (string)this["autoOffsetReset"];
            }
        }

        [ConfigurationProperty("consumerTimeoutMs", IsRequired = false, DefaultValue = ConsumerConfig.DefaultConsumerTimeoutMs)]
        public int ConsumerTimeoutMs
        {
            get
            {
                return (int)this["consumerTimeoutMs"];
            }
        }

        [ConfigurationProperty("clientId", IsRequired = false, DefaultValue = null)]
        public string ClientId
        {
            get
            {
                if (this["clientId"] == null)
                {
                    return this.GroupId;
                }

                return (string)this["clientId"];
            }
        }

        [ConfigurationProperty("zookeeper", IsRequired = false, DefaultValue = null)]
        public ZooKeeperConfigurationElement ZooKeeperServers
        {
            get
            {
                return (ZooKeeperConfigurationElement)this["zookeeper"];
            }
        }

        public static ConsumerConfigurationSection FromXml(XElement element)
        {
            var section = new ConsumerConfigurationSection();
            section.DeserializeSection(element.CreateReader());
            return section;
        }
    }
}
