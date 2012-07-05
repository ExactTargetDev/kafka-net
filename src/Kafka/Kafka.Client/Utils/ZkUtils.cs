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

using System;

namespace Kafka.Client.Utils
{
    using System.Collections.Generic;
    using System.Globalization;
    using System.Reflection;
    using Kafka.Client.Cluster;
    using Kafka.Client.ZooKeeperIntegration;
    using log4net;
    using ZooKeeperNet;
    using System.Linq;

    internal class ZkUtils
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private const string BrokerTopicsPath = "/brokers/topics";

        internal static void UpdatePersistentPath(IZooKeeperClient zkClient, string path, string data)
        {
            try
            {
                zkClient.WriteData(path, data);
            }
            catch (KeeperException.NoNodeException)
            {
                CreateParentPath(zkClient, path);

                try
                {
                    zkClient.CreatePersistent(path, data);
                }
                catch (KeeperException.NodeExistsException)
                {
                    zkClient.WriteData(path, data);
                }
            }
        }

        internal static void CreateParentPath(IZooKeeperClient zkClient, string path)
        {
            string parentDir = path.Substring(0, path.LastIndexOf('/'));
            if (parentDir.Length != 0)
            {
                zkClient.CreatePersistent(parentDir, true);
            }
        }

        internal static string GetConsumerPartitionOwnerPath(string group, string topic, string partition)
        {
            var topicDirs = new ZKGroupTopicDirs(group, topic);
            return topicDirs.ConsumerOwnerDir + "/" + partition;
        }

        internal static void DeletePath(IZooKeeperClient zkClient, string path)
        {
            try
            {
                zkClient.Delete(path);
            }
            catch (KeeperException.NoNodeException)
            {
                Logger.InfoFormat(CultureInfo.CurrentCulture, "{0} deleted during connection loss; this is ok", path);
            }
        }

        internal static IDictionary<string, IList<string>> GetPartitionsForTopics(IZooKeeperClient zkClient, IEnumerable<string> topics)
        {
            var result = new Dictionary<string, IList<string>>();
            foreach (string topic in topics)
            {
                var partitions = zkClient.GetChildrenParentMayNotExist(GetTopicPartitionsPath(topic));
                Logger.DebugFormat("children of /brokers/topics/{0} are {1}", topic, partitions);
                result.Add(topic, partitions != null ? partitions.OrderBy(x => x).ToList() : new List<string>());
            }

            return result;
        }

        private static string GetTopicPartitionPath(string topic, string partitionId)
        {
            return GetTopicPartitionsPath(topic) + "/" + partitionId;
        }

        private static string GetTopicPartitionsPath(string topic)
        {
            return GetTopicPath(topic) + "/partitions";
        }

        private static string GetTopicPath(string topic)
        {
            return BrokerTopicsPath + "/" + topic;
        }

        private static string GetTopicPartitionReplicasPath(string topic, string partitionId)
        {
            return GetTopicPartitionPath(topic, partitionId) + "/" + "replicas";
        }

        private static string GetTopicPartitionLeaderPath(string topic, string partitionId)
        {
            return GetTopicPartitionPath(topic, partitionId) + "/" + "leader";
        }

        internal static void CreateEphemeralPathExpectConflict(IZooKeeperClient zkClient, string path, string data)
        {
            try
            {
                CreateEphemeralPath(zkClient, path, data);
            }
            catch (KeeperException.NodeExistsException)
            {
                string storedData;
                try
                {
                    storedData = zkClient.ReadData<string>(path);
                }
                catch (KeeperException.NoNodeException)
                {
                    // the node disappeared; treat as if node existed and let caller handles this
                    throw;
                }

                if (storedData == null || storedData != data)
                {
                    Logger.InfoFormat(CultureInfo.CurrentCulture, "conflict in {0} data: {1} stored data: {2}", path, data, storedData);
                    throw;
                }
                else
                {
                    // otherwise, the creation succeeded, return normally
                    Logger.InfoFormat(CultureInfo.CurrentCulture, "{0} exits with value {1} during connection loss; this is ok", path, data);
                }
            }
        }

        internal static void CreateEphemeralPath(IZooKeeperClient zkClient, string path, string data)
        {
            try
            {
                zkClient.CreateEphemeral(path, data);
            }
            catch (KeeperException.NoNodeException)
            {
                ZkUtils.CreateParentPath(zkClient, path);
                zkClient.CreateEphemeral(path, data);
            }
        }

        internal static IEnumerable<Broker> GetAllBrokersInCluster(IZooKeeperClient zkClient)
        {
            var brokerIds = zkClient.GetChildren(ZooKeeperClient.DefaultBrokerIdsPath).OrderBy(x => x).ToList();
            return ZkUtils.GetBrokerInfoFromIds(zkClient, brokerIds.Select(x => int.Parse(x)));
        }

        internal static int? GetLeaderForPartition(IZooKeeperClient zkClient, string topic, int partition)
        {
            var leader = zkClient.ReadData<string>(GetTopicPartitionLeaderPath(topic, partition.ToString()), true);
            if (leader == null)
            {
                return (int?) null;
            }
            else
            {
                return int.Parse(leader);
            }
        }

        internal static IEnumerable<Broker> GetBrokerInfoFromIds(IZooKeeperClient zkClient, IEnumerable<int> brokerIds)
        {
            return brokerIds.Select(
                brokerId =>
                Broker.CreateBroker(brokerId,
                                    zkClient.ReadData<string>(ZooKeeperClient.DefaultBrokerIdsPath + "/" + brokerId)));
        }
    }
}
