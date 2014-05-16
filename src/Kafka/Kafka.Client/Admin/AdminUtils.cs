namespace Kafka.Client.Admin
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    using Kafka.Client.Common;
    using Kafka.Client.Utils;
    using Kafka.Client.ZKClient;

    using System.Linq;

    using Newtonsoft.Json.Linq;

    using log4net;

    public static class AdminUtils
    {
         static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private static Random rand = new Random();

        public static Dictionary<int, List<int>> AssignReplicasToBrokers(
            List<int> brokerList,
            int nPartitions,
            int replicationFactor,
            int fixedStartIndex = -1,
            int startPartitonId = -1)
        {
            if (nPartitions <= 0)
            {
                throw new ArgumentException("number of partitions must be larger than 0", "nPartitions");
            }
            if (replicationFactor <= 0)
            {
                throw new ArgumentException("replication factor must be larger than 0", "replicationFactor");
            }
            if (replicationFactor > brokerList.Count)
            {
                throw new Exception("replication factor: " + replicationFactor + " larger than available brokers: " + brokerList.Count);
            }
            var ret = new Dictionary<int, List<int>>();
            var startIndex = (fixedStartIndex >= 0) ? fixedStartIndex : rand.Next(brokerList.Count);
            var currentPartitionId = startPartitonId >= 0 ? startPartitonId : 0;

            var nextReplicaShift = fixedStartIndex > -0 ? fixedStartIndex : rand.Next(brokerList.Count);
            for (var i = 0; i < nPartitions; i++)
            {
                if (currentPartitionId > 0 && (currentPartitionId % brokerList.Count == 0))
                {
                    nextReplicaShift++;
                }
                var firstReplicaIndex = (currentPartitionId + startIndex) % brokerList.Count;
                var replicaList = new List<int> { brokerList[firstReplicaIndex] };
                for (var j = 0; j < replicationFactor - 1; j++)
                {
                    replicaList.Add(brokerList[ReplicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.Count)]);
                }
                ret[currentPartitionId] = replicaList;
                currentPartitionId++;
            }

            return ret;
        }

        public static void CreateTopic(
            ZkClient zkClient,
            string topic,
            int partitions,
            int replicationFactor,
            Dictionary<string, string> topicConfig)
        {
            var brokerList = ZkUtils.GetSortedBrokerList(zkClient);
            var replicaAssigment = AssignReplicasToBrokers(brokerList, partitions, replicationFactor);
            CreateOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, replicaAssigment, topicConfig);
        }

         public static void CreateOrUpdateTopicPartitionAssignmentPathInZK(
             ZkClient zkClient,
             string topic,
             Dictionary<int, List<int>> partitionReplicaAssignment,
             Dictionary<string, string> config,
             bool update = false)
         {

             //Note: validation was omited
             var topicPath = ZkUtils.GetTopicPath(topic);
             if (!update && zkClient.Exists(topicPath))
             {
                 throw new KafkaException("Topic " + topic +" already exists.");
             }

             // write out the config if there is any, this isn't transactional with the partition assignments
             WriteTopicConfig(zkClient, topic, config);
    
            // create the partition assignment
             WriteTopicPartitionAssignment(zkClient, topic, partitionReplicaAssignment, update);
         }

        private static void WriteTopicPartitionAssignment(
            ZkClient zkClient, string topic, Dictionary<int, List<int>> replicaAssignment, bool update)
        {
            var zkPath = ZkUtils.GetTopicPath(topic);
            var jsonPartitonData =
                ZkUtils.ReplicaAssignmentZkData(replicaAssignment.ToDictionary(x => x.Key.ToString(), v => v.Value));

            if (!update)
            {
                Logger.InfoFormat("Topic creation: {0}", jsonPartitonData);
                ZkUtils.CreatePersistentPath(zkClient, zkPath, jsonPartitonData);
            }
            else
            {
                Logger.InfoFormat("Topic update {0}" + jsonPartitonData);
                ZkUtils.UpdatePersistentPath(zkClient, zkPath, jsonPartitonData);
            }
            Logger.DebugFormat("Updated path {0} with {1} for replica assignment", zkPath, jsonPartitonData);
        }

        private static void WriteTopicConfig(ZkClient zkClient, string topic, Dictionary<string, string> config)
        {
            var map = new { version = 1, config = config };

            ZkUtils.UpdatePersistentPath(zkClient, ZkUtils.GetTopicConfigPath(topic), JObject.FromObject(map).ToString());
        }

        private static int ReplicaIndex(int firstReplicaIndex, int secondReplicaShirt, int replicaIndex, int nBrokers)
        {
            var shift = 1 + (secondReplicaShirt + replicaIndex) % (nBrokers - 1);
            return (firstReplicaIndex + shift) % nBrokers;
        }
    }
}