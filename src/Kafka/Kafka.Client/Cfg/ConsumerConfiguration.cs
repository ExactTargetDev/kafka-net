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
    using Kafka.Client.Api;
    using System.Configuration;
    using System.Net;
    using System.Globalization;
    using System.Text;
    using System.Xml.Linq;


    /// <summary>
    /// Configuration used by the consumer
    /// </summary>
    /// TODO: review and update props
    public class ConsumerConfiguration
    {
        public const int RefreshMetadataBackoffMs = 200;

        public const int SocketTimeout = 30 * 1000;

        public const int SocketBufferSize = 64 * 1024;

        public const int FetchSize = 1024 * 1024;

        public const int MaxFetchSize = 10 * FetchSize;

        public const int DefaultNumConsumerFetchers = 1;

        public const int DefaultFetcherBackoffMs = 1000;

        public const bool AutoCommit = true;

        public const int AutoCommitInterval = 60 * 1000;

        public const int MaxQueuedChunks = 2;

        public const int MaxRebalanceRetries = 4;

        public const string DefaultAutoOffsetReset = OffsetRequest.LargestTimeString;

        public const int DefaultConsumerTimeoutMs = -1;

        public const int MinFetchBytes = 1;

        public const int MaxFetchWaitMs = 100;

        public const string MirrorTopicsWhitelist = "";

        public const string MirrorTopicsBlacklist = "";

        public const int MirrorConsumerNumThreads = 1;

        public const string DefaultClientId = "";

        public ConsumerConfiguration()
        {
            this.GroupId = string.Empty;
            this.ConsumerId = null;
            this.SocketTimeoutMs = SocketTimeout;
            this.SocketReceiveBufferBytes = SocketBufferSize;
            this.FetchMessageMaxBytes = FetchSize;
            this.NumConsumerFetchers = DefaultNumConsumerFetchers;
            this.AutoCommitEnable = AutoCommit;
            this.AutoCommitIntervalMs = AutoCommitInterval;
            this.QueuedMaxMessages = MaxQueuedChunks;
            this.RebalanceMaxRetries = MaxRebalanceRetries;
            this.FetchMinBytes = MinFetchBytes;
            this.FetchWaitMaxMs = MaxFetchWaitMs;
            this.RebalanceBackoffMs = ZooKeeperConfiguration.DefaultSyncTime;
            this.RefreshLeaderBackoffMs = RefreshMetadataBackoffMs;
            this.AutoOffsetReset = DefaultAutoOffsetReset;
            this.ConsumerTimeoutMs = DefaultConsumerTimeoutMs;
            this.ClientId = GroupId;
        }

        public ConsumerConfiguration(string host, int port, string groupId)
            : this()
        {
            this.ZooKeeper = new ZooKeeperConfiguration(host + ":" + port, ZooKeeperConfiguration.DefaultSessionTimeout, 
                ZooKeeperConfiguration.DefaultConnectionTimeout, ZooKeeperConfiguration.DefaultSyncTime);
            this.GroupId = groupId;
        }

        public ConsumerConfiguration(ConsumerConfigurationSection config)
        {
            Validate(config);
            this.GroupId = config.GroupId;
            this.ConsumerId = config.ConsumerId;
            this.SocketTimeoutMs = config.SocketTimeout;
            this.SocketReceiveBufferBytes = config.SocketBufferSize;
            this.FetchMessageMaxBytes = config.FetchSize;
            this.NumConsumerFetchers = config.NumConsumerFetchers;
            this.AutoCommitEnable = config.AutoCommit;
            this.AutoCommitIntervalMs = config.AutoCommitInterval;
            this.QueuedMaxMessages = config.MaxQueuedChunks;
            this.RebalanceMaxRetries = config.MaxRebalanceRetries;
            this.FetchMinBytes = config.MinFetchBytes;
            this.FetchWaitMaxMs = config.MaxFetchWaitMs;
            this.RebalanceBackoffMs = config.RebalanceBackoffMs;
            this.RefreshLeaderBackoffMs = config.RefreshMetadataBackoffMs;
            this.AutoOffsetReset = config.AutoOffsetReset;
            this.ConsumerTimeoutMs = config.ConsumerTimeoutMs;
            this.ClientId = config.GroupId;
          
                this.SetZooKeeperConfiguration(config.ZooKeeperServers);
        }

        public ConsumerConfiguration(XElement xmlElement)
            : this(ConsumerConfigurationSection.FromXml(xmlElement))
        {
        }

        public string GroupId { get; set; }

        public string ConsumerId { get; set; }

        public int SocketTimeoutMs { get; set; }

        public int SocketReceiveBufferBytes  { get; set; }

        public int FetchMessageMaxBytes { get; private set; } //TODO: public setters

        public int NumConsumerFetchers { get; private set; }

        public bool AutoCommitEnable { get; private set; }

        public int AutoCommitIntervalMs { get; private set; }

        public int QueuedMaxMessages { get; private set; }

        public int RebalanceMaxRetries { get; private set; }

        public int FetchMinBytes { get; private set; }

        public int FetchWaitMaxMs { get; private set; }

        public int RebalanceBackoffMs { get; private set; }

        public int RefreshLeaderBackoffMs { get; private set; }

        public string AutoOffsetReset { get; private set; }

        public int ConsumerTimeoutMs { get; private set; }

        public string ClientId { get; set; }


        public static ConsumerConfiguration Configure(string section)
        {
            var config = ConfigurationManager.GetSection(section) as ConsumerConfigurationSection;
            return new ConsumerConfiguration(config);
        }

       

        public ZooKeeperConfiguration ZooKeeper { get; set; }

        public BrokerConfiguration Broker { get; set; }

        private static void Validate(ConsumerConfigurationSection config)
        {
           //TODO:
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

        private void SetBrokerConfiguration(BrokerConfigurationElement config)
        {
            this.Broker = new BrokerConfiguration
            {
                BrokerId = config.Id,
                Host = GetIpAddress(config.Host),
                Port = config.Port
            };
        }

        private void SetZooKeeperConfiguration(ZooKeeperConfigurationElement config)
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
    }
}
