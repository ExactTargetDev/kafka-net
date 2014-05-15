namespace Kafka.Client.Admin
{
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
    }
}