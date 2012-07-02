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
    public class ProducerConfiguration : ISyncProducerConfigShared, IAsyncProducerConfigShared
    {
        public const ProducerTypes DefaultProducerType = ProducerTypes.Sync;
        public const string DefaultPartitioner = "Kafka.Client.Producers.Partitioning.DefaultPartitioner`1";
        public const string DefaultSerializer = "Kafka.Client.Serialization.StringEncoder";
        public const string DefaultSectionName = "kafkaProducer";
        public const int DefaultProducerRetries = 3;
        public const int DefaultProducerRetryBackoffMiliseconds = 5;

        public static ProducerConfiguration Configure(string section)
        {
            var config = ConfigurationManager.GetSection(section) as ProducerConfigurationSection;
            return new ProducerConfiguration(config);
        }

        private ProducerConfiguration()
        {
            this.ProducerType = DefaultProducerType;
            this.BufferSize = SyncProducerConfiguration.DefaultBufferSize;
            this.ConnectTimeout = SyncProducerConfiguration.DefaultConnectTimeout;
            this.SocketTimeout = SyncProducerConfiguration.DefaultSocketTimeout;
            this.MaxMessageSize = SyncProducerConfiguration.DefaultMaxMessageSize;
            this.SerializerClass = AsyncProducerConfiguration.DefaultSerializerClass;
            this.CompressionCodec = CompressionCodecs.DefaultCompressionCodec;
            this.CompressedTopics = new List<string>();
            this.ProducerRetries = DefaultProducerRetries;
            this.ProducerRetryBackoffMiliseconds = DefaultProducerRetryBackoffMiliseconds;
            this.CorrelationId = SyncProducerConfiguration.DefaultCorrelationId;
            this.ClientId = SyncProducerConfiguration.DefaultClientId;
            this.RequiredAcks = SyncProducerConfiguration.DefaultRequiredAcks;
            this.AckTimeout = SyncProducerConfiguration.DefaultAckTimeout;
        }

        public ProducerConfiguration(XElement xml) : this(ProducerConfigurationSection.FromXml(xml))
        {
        }

        public ProducerConfiguration(IList<BrokerConfiguration> brokersConfig)
            : this()
        {
            this.Brokers = brokersConfig;
        }

        public ProducerConfiguration(ZooKeeperConfiguration zooKeeperConfig)
            : this()
        {
            this.ZooKeeper = zooKeeperConfig;
            this.PartitionerClass = DefaultPartitioner;
            this.SerializerClass = DefaultSerializer;
        }

        public ProducerConfiguration(ProducerConfigurationSection config)
        {
            Guard.NotNull(config, "config");
            this.ProducerType = config.ProducerType;
            this.BufferSize = config.BufferSize;
            this.ConnectTimeout = config.ConnectionTimeout;
            this.SocketTimeout = config.SocketTimeout;
            this.MaxMessageSize = config.MaxMessageSize;
            this.SerializerClass = config.Serializer;
            this.CompressionCodec = config.CompressionCodec;
            this.CompressedTopics = config.CompressedTopics ?? new List<string>();
            this.ProducerRetries = config.ProducerRetries;
            this.ProducerRetryBackoffMiliseconds = config.ProducerRetryBackoffMiliseconds;
            this.CorrelationId = config.CorrelationId;
            this.ClientId = config.ClientId;
            this.RequiredAcks = config.RequiredAcks;
            this.AckTimeout = config.AckTimeout;
            Validate(config);
            if (config.ZooKeeperServers.ElementInformation.IsPresent)
            {
                this.SetZooKeeperServers(config.ZooKeeperServers);
                this.PartitionerClass = config.Partitioner;
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
                if (value != null)
                {
                    this.zooKeeper = null;
                    this.partitionerClass = null;
                }

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
                if (value != null)
                {
                    if (this.broker != null)
                    {
                        throw new ConfigurationErrorsException("Partitioner cannot be used when broker list is set");
                    }
                }

                this.partitionerClass = value;
            }
        }

        public ProducerTypes ProducerType { get; set; }

        public int BufferSize { get; set; }

        public int ConnectTimeout { get; set; }

        public int SocketTimeout { get; set; }

        public int MaxMessageSize { get; set; }

        public string SerializerClass { get; set; }

        public string CallbackHandlerClass { get; set; }

        public CompressionCodecs CompressionCodec { get; set; }

        public IEnumerable<string> CompressedTopics { get; set; }

        public int ProducerRetries { get; set; }

        public int ProducerRetryBackoffMiliseconds { get; set; }

        public int CorrelationId { get; set; }

        public string ClientId { get; set; }

        public short RequiredAcks { get; set; }

        public int AckTimeout { get; set; }

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
                throw new ConfigurationErrorsException("Partitioner cannot be used when broker list is set");
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
