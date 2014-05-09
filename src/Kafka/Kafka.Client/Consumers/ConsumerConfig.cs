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
namespace Kafka.Client.Consumers
{
    using System.Configuration;
    using System.Globalization;
    using System.Net;
    using System.Text;
    using System.Xml.Linq;

    using Kafka.Client.Api;
    using Kafka.Client.Cfg.Elements;
    using Kafka.Client.Cfg.Sections;
    using Kafka.Client.Common;
    using Kafka.Client.Utils;

    /// <summary>
    /// Configuration used by the consumer
    /// </summary>
    public class ConsumerConfig : Config
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
            this.ClientId = this.GroupId;
        }

        public ConsumerConfig(string host, int port, string groupId)
            : this()
        {
            this.ZooKeeper = new ZkConfig(
                host + ":" + port,
                ZkConfig.DefaultSessionTimeout,
                ZkConfig.DefaultConnectionTimeout,
                ZkConfig.DefaultSyncTime);
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

        /// <summary>
        /// a string that uniquely identifies a set of consumers within the same consumer group
        /// </summary>
        public string GroupId { get; set; }

        /// <summary>
        /// consumer id: generated automatically if not set.
        /// Set this explicitly for only testing purpose.
        /// </summary>
        public string ConsumerId { get; set; }

        /// <summary>
        /// the socket timeout for network requests. The actual timeout set will be max.fetch.wait + socket.timeout.ms.
        /// </summary>
        public int SocketTimeoutMs { get; set; }

        /// <summary>
        /// the socket receive buffer for network requests 
        /// </summary>
        public int SocketReceiveBufferBytes { get; set; }

        /// <summary>
        /// the number of byes of messages to attempt to fetch
        /// </summary>
        public int FetchMessageMaxBytes { get; set; } 

        /// <summary>
        /// the number threads used to fetch data
        /// </summary>
        public int NumConsumerFetchers { get; set; }

        /// <summary>
        /// if true, periodically commit to zookeeper the offset of messages already fetched by the consumer
        /// </summary>
        public bool AutoCommitEnable { get; set; }

        /// <summary>
        /// the frequency in ms that the consumer offsets are committed to zookeeper
        /// </summary>
        public int AutoCommitIntervalMs { get; set; }

        /// <summary>
        /// max number of message chunks buffered for consumption, each chunk can be up to fetch.message.max.bytes
        /// </summary>
        public int QueuedMaxMessages { get; set; }

        /// <summary>
        /// max number of retries during rebalance
        /// </summary>
        public int RebalanceMaxRetries { get; set; }

        /// <summary>
        /// the minimum amount of data the server should return for a fetch request. If insufficient data is available the request will block 
        /// </summary>
        public int FetchMinBytes { get;  set; }

        /// <summary>
        /// the maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy fetch.min.bytes
        /// </summary>
        public int FetchWaitMaxMs { get; set; }

        /// <summary>
        /// backoff time between retries during rebalance
        /// </summary>
        public int RebalanceBackoffMs { get; set; }

        /// <summary>
        /// backoff time to refresh the leader of a partition after it loses the current leader
        /// </summary>
        public int RefreshLeaderBackoffMs { get; set; }

        /// <summary>
        /// what to do if an offset is out of range.
        /// smallest : automatically reset the offset to the smallest offset
        /// largest : automatically reset the offset to the largest offset
        /// anything else: throw exception to the consumer
        /// </summary>
        public string AutoOffsetReset { get; set; }

        /// <summary>
        /// throw a timeout exception to the consumer if no message is available for consumption after the specified interval
        /// </summary>
        public int ConsumerTimeoutMs { get; set; }

        /// <summary>
        /// Client id is specified by the kafka consumer client, used to distinguish different clients
        /// </summary>
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
                throw new ConfigurationErrorsException("Wrong value " + autoOffsetReset + "of auto.reset.offset in ConsumerConfig. "
                                                 + "Valid values are: " + OffsetRequest.SmallestTimeString + " and " + OffsetRequest.LargestTimeString);
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
