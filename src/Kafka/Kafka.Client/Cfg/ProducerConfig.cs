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

using Kafka.Client.Messages;

namespace Kafka.Client.Cfg
{
    using System.Collections.Generic;
    using System.Configuration;
    using System.Globalization;
    using System.Net;
    using System.Text;
    using Kafka.Client.Producers;
    using Kafka.Client.Utils;
    using System.Xml.Linq;


    /// <summary>
    /// High-level API configuration for the producer
    /// </summary>
    public class ProducerConfig : AsyncProducerConfiguration, ISyncProducerConfigShared, IAsyncProducerConfigShared
    {
        public const string DefaultPartitioner = "Kafka.Client.Producers.DefaultPartitioner";

        public const ProducerTypes DefaultProducerType = ProducerTypes.Sync;

        public const CompressionCodecs DefaultCompressionCodec = CompressionCodecs.NoCompressionCodec;

        public const int DefaultMessageSendRetries = 3;

        public const int DefaultRetryBackoffMs = 100;

        public const string DefaultSectionName = "kafkaProducer";

        public const int DefaultTopicMetadataRefreshIntervalMs = 600000;
        
        public static ProducerConfig Configure(string section)
        {
            var config = ConfigurationManager.GetSection(section) as ProducerConfigurationSection;
            return new ProducerConfig(config);
        }

        public ProducerConfig()
        {
            this.ProducerType = DefaultProducerType;
            this.PartitionerClass = DefaultPartitioner;
            this.CompressedTopics = new List<string>();
            this.MessageSendMaxRetries = DefaultMessageSendRetries;
            this.RetryBackoffMs = DefaultRetryBackoffMs;
            this.TopicMetadataRefreshIntervalMs = DefaultTopicMetadataRefreshIntervalMs;
        }

        public ProducerConfig(XElement xml) : this(ProducerConfigurationSection.FromXml(xml))
        {
        }

        public ProducerConfig(IList<BrokerConfiguration> brokersConfig)
            : this()
        {
            this.Brokers = brokersConfig;
        }

        public ProducerConfig(ProducerConfigurationSection config) : base(config, string.Empty, 0)
        {
            this.ProducerType = config.ProducerType;
            this.CompressionCodec = config.CompressionCodec;
            this.CompressedTopics = config.CompressedTopics;
            this.MessageSendMaxRetries = config.MessageSendMaxRetries;
            this.RetryBackoffMs = config.RetryBackoffMs;
            this.TopicMetadataRefreshIntervalMs = config.TopicMetadataRefreshIntervalMs;
            Validate(config);

            this.PartitionerClass = config.Partitioner;

            if (config.ZooKeeperServers.ElementInformation.IsPresent)
            {
                this.SetZooKeeperServers(config.ZooKeeperServers);
            }
            else
            {
                this.SetKafkaBrokers(config.Brokers);
            }
        }

        /// <summary>
        /// Gets a value indicating whether ZooKeeper based automatic broker discovery is enabled.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is zoo keeper enabled; otherwise, <c>false</c>.
        /// </value>
        public bool IsZooKeeperEnabled
        {
            get
            {
                return this.ZooKeeper != null;
            }
        }

        private IList<BrokerConfiguration> broker;

        public IList<BrokerConfiguration> Brokers
        {
            get
            {
                return this.broker;
            }

            set
            {
                this.broker = value;
            }
        }

        private ZooKeeperConfiguration zooKeeper;

        public ZooKeeperConfiguration ZooKeeper
        {
            get
            {
                return this.zooKeeper;
            }

            set
            {
                if (value != null)
                {
                    this.broker = null;
                }

                this.zooKeeper = value;
            }
        }

        private string partitionerClass;

        public string PartitionerClass
        {
            get
            {
                return this.partitionerClass;
            }

            set
            {
                this.partitionerClass = value;
            }
        }

        public ProducerTypes ProducerType { get; set; }

        public CompressionCodecs CompressionCodec { get; set; }

        public IEnumerable<string> CompressedTopics { get; set; }

        public int MessageSendMaxRetries { get; set; }

        public int RetryBackoffMs { get; set; }

        public int TopicMetadataRefreshIntervalMs { get; set; }

        private void SetZooKeeperServers(ZooKeeperConfigurationElement config)
        {
            if (config.Servers.Count == 0)
            {
                throw new ConfigurationErrorsException();
            }

            var sb = new StringBuilder();
            foreach (ZooKeeperServerConfigurationElement server in config.Servers)
            {
                sb.Append(GetIpAddress(server.Host));
                sb.Append(':');
                sb.Append(server.Port);
                sb.Append(',');
            }

            sb.Remove(sb.Length - 1, 1);
            this.ZooKeeper = new ZooKeeperConfiguration(
                sb.ToString(),
                config.SessionTimeout,
                config.ConnectionTimeout,
                config.SyncTime);
        }

        private void SetKafkaBrokers(BrokerConfigurationElementCollection brokersColl)
        {
            this.Brokers = new List<BrokerConfiguration>();
            foreach (BrokerConfigurationElement broker in brokersColl)
            {
                this.Brokers.Add(
                    new BrokerConfiguration
                    {
                        BrokerId = broker.Id,
                        Host = GetIpAddress(broker.Host),
                        Port = broker.Port
                    });
            }
        }

        private static void Validate(ProducerConfigurationSection config)
        {
            if (config.ZooKeeperServers.ElementInformation.IsPresent
                && config.Brokers.ElementInformation.IsPresent)
            {
                throw new ConfigurationErrorsException("ZooKeeper configuration cannot be set when brokers configuration is used");
            }

            if (!config.ZooKeeperServers.ElementInformation.IsPresent
                && !config.Brokers.ElementInformation.IsPresent)
            {
                throw new ConfigurationErrorsException("ZooKeeper server or Kafka broker configuration must be set");
            }

            if (config.ZooKeeperServers.ElementInformation.IsPresent
                && config.ZooKeeperServers.Servers.Count == 0)
            {
                throw new ConfigurationErrorsException("At least one ZooKeeper server address is required");
            }

            if (config.Brokers.ElementInformation.IsPresent
                && config.Brokers.Count == 0)
            {
                throw new ConfigurationErrorsException("Brokers list is empty");
            }

            if (config.Brokers.ElementInformation.IsPresent
                && config.Partitioner != DefaultPartitioner)
            {
                throw new ConfigurationErrorsException("IPartitioner cannot be used when broker list is set");
            }
        }

        private static string GetIpAddress(string host)
        {
            IPAddress ipAddress;
            if (!IPAddress.TryParse(host, out ipAddress))
            {
                IPHostEntry ip = Dns.GetHostEntry(host);
                if (ip.AddressList.Length > 0)
                {
                    return ip.AddressList[0].ToString();
                }

                throw new ConfigurationErrorsException(string.Format(CultureInfo.CurrentCulture, "Could not resolve the zookeeper server address: {0}.", host));
            }

            return host;
        }
    }
}
