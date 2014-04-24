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
namespace Kafka.Client.Cfg
{
    using System.Configuration;
    using System.Xml.Linq;

    using Kafka.Client.Api;


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

        [ConfigurationProperty("socketTimeout", IsRequired = false, DefaultValue = ConsumerConfiguration.SocketTimeout)]
        public int SocketTimeout
        {
            get
            {
                return (int)this["socketTimeout"];
            }
        }

        [ConfigurationProperty("socketBufferSize", IsRequired = false, DefaultValue = ConsumerConfiguration.SocketBufferSize)]
        public int SocketBufferSize
        {
            get
            {
                return (int)this["socketBufferSize"];
            }
        }

        [ConfigurationProperty("fetchSize", IsRequired = false, DefaultValue = ConsumerConfiguration.FetchSize)]
        public int FetchSize
        {
            get
            {
                return (int)this["fetchSize"];
            }
        }

        [ConfigurationProperty("numConsumerFetchers", IsRequired = false, DefaultValue = ConsumerConfiguration.DefaultNumConsumerFetchers)]
        public int NumConsumerFetchers
        {
            get
            {
                return (int)this["numConsumerFetchers"];
            }
        }

        [ConfigurationProperty("autoCommit", IsRequired = false, DefaultValue = ConsumerConfiguration.AutoCommit)]
        public bool AutoCommit
        {
            get
            {
                return (bool)this["autoCommit"];
            }
        }

        [ConfigurationProperty("autoCommitInterval", IsRequired = false, DefaultValue = ConsumerConfiguration.AutoCommitInterval)]
        public int AutoCommitInterval
        {
            get
            {
                return (int)this["autoCommitInterval"];
            }
        }

        [ConfigurationProperty("maxQueuedChunks", IsRequired = false, DefaultValue = ConsumerConfiguration.MaxQueuedChunks)]
        public int MaxQueuedChunks
        {
            get
            {
                return (int)this["maxQueuedChunks"];
            }
        }

        [ConfigurationProperty("maxRebalanceRetries", IsRequired = false, DefaultValue = ConsumerConfiguration.MaxRebalanceRetries)]
        public int MaxRebalanceRetries
        {
            get
            {
                return (int)this["maxRebalanceRetries"];
            }
        }

        [ConfigurationProperty("minFetchBytes", IsRequired = false, DefaultValue = ConsumerConfiguration.MinFetchBytes)]
        public int MinFetchBytes
        {
            get
            {
                return (int)this["minFetchBytes"];
            }
        }

        [ConfigurationProperty("maxFetchWaitMs", IsRequired = false, DefaultValue = ConsumerConfiguration.MaxFetchWaitMs)]
        public int MaxFetchWaitMs
        {
            get
            {
                return (int)this["maxFetchWaitMs"];
            }
        }

        [ConfigurationProperty("rebalanceBackoffMs", IsRequired = false, DefaultValue = ZooKeeperConfiguration.DefaultSyncTime)]
        public int RebalanceBackoffMs
        {
            get
            {
                return (int)this["rebalanceBackoffMs"];
            }
        }

        [ConfigurationProperty("refreshMetadataBackoffMs", IsRequired = false, DefaultValue = ConsumerConfiguration.RefreshMetadataBackoffMs)]
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

        [ConfigurationProperty("consumerTimeoutMs", IsRequired = false, DefaultValue = ConsumerConfiguration.DefaultConsumerTimeoutMs)]
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
                    return GroupId;
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
