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
    using System.Globalization;
    using System.Net;
    using System.Text;
    using System.Xml.Linq;

    using Kafka.Client.Api;
    using Kafka.Client.Cfg.Elements;
    using Kafka.Client.Cfg.Sections;
    using Kafka.Client.Utils;

    /// <summary>
    /// Configuration used by the consumer
    /// </summary>
    public class ConsumerConfig
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

        public ConsumerConfig()
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
            this.RebalanceBackoffMs = ZkConfig.DefaultSyncTime;
            this.RefreshLeaderBackoffMs = RefreshMetadataBackoffMs;
            this.AutoOffsetReset = DefaultAutoOffsetReset;
            this.ConsumerTimeoutMs = DefaultConsumerTimeoutMs;
            this.ClientId = GroupId;
        }

        public ConsumerConfig(string host, int port, string groupId)
            : this()
        {
            this.ZooKeeper = new ZkConfig(host + ":" + port, ZkConfig.DefaultSessionTimeout, 
                ZkConfig.DefaultConnectionTimeout, ZkConfig.DefaultSyncTime);
            this.GroupId = groupId;
        }

        public ConsumerConfig(ConsumerConfigurationSection config)
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

        public ConsumerConfig(XElement xmlElement)
            : this(ConsumerConfigurationSection.FromXml(xmlElement))
        {
        }

        public string GroupId { get; set; }

        public string ConsumerId { get; set; }

        public int SocketTimeoutMs { get; set; }

        public int SocketReceiveBufferBytes  { get; set; }

        public int FetchMessageMaxBytes { get; set; } 

        public int NumConsumerFetchers { get; set; }

        public bool AutoCommitEnable { get; set; }

        public int AutoCommitIntervalMs { get; set; }

        public int QueuedMaxMessages { get; set; }

        public int RebalanceMaxRetries { get; set; }

        public int FetchMinBytes { get;  set; }

        public int FetchWaitMaxMs { get; set; }

        public int RebalanceBackoffMs { get; set; }

        public int RefreshLeaderBackoffMs { get; set; }

        public string AutoOffsetReset { get; set; }

        public int ConsumerTimeoutMs { get; set; }

        public string ClientId { get; set; }


        public static ConsumerConfig Configure(string section)
        {
            var config = ConfigurationManager.GetSection(section) as ConsumerConfigurationSection;
            return new ConsumerConfig(config);
        }

       

        public ZkConfig ZooKeeper { get; set; }

        private static void Validate(ConsumerConfigurationSection config)
        {
            ValidateClientId(config.ClientId);
            ValidateGroupId(config.GroupId);
            ValidateAutoOffsetReset(config.AutoOffsetReset);
        }

        public static void ValidateClientId(string clientId)
        {
            ValidateChars("client.id", clientId);
        }

        public static void ValidateGroupId(string groupId)
        {
            ValidateChars("group.id", groupId);
        }

        public static void ValidateAutoOffsetReset(string autoOffsetReset)
        {
            if (OffsetRequest.SmallestTimeString != autoOffsetReset
                && OffsetRequest.LargestTimeString != autoOffsetReset)
            {
                throw new ConfigurationException("Wrong value " + autoOffsetReset + "of auto.reset.offset in ConsumerConfig. "
                                                 + "Valid values are: " + OffsetRequest.SmallestTimeString + " and " + OffsetRequest.LargestTimeString);
            }
        }

        public static void ValidateChars(string prop, string value)
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
            this.ZooKeeper = new ZkConfig(
                sb.ToString(),
                config.SessionTimeout,
                config.ConnectionTimeout,
                config.SyncTime);
        }
    }
}
