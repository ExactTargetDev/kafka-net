namespace Kafka.Client.Utils
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Threading;

    using Kafka.Client.Clusters;
    using Kafka.Client.Consumers;
    using Kafka.Client.ZKClient;
    using Kafka.Client.ZKClient.Exceptions;

    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    using Org.Apache.Zookeeper.Data;

    using log4net;

    using Kafka.Client.Extensions;

    public static class ZkUtils
    {

        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public const string ConsumersPath = "/consumers";

        public const string BrokerIdsPath = "/brokers/ids";

        public const string BrokerTopicsPath = "/brokers/topics";

        public const string TopicConfigPath = "/config/topics";

        public const string TopicConfigChangesPath = "/config/changes";

        public const string ControllerPath = "/controller";

        public const string ControllerEpochPath = "/controller_epoch";

        public const string ReassignPartitionsPath = "/admin/reassign_partitions";

        public const string DeleteTopicsPath = "/admin/delete_topics";

        public const string PreferredReplicaLeaderElectionPath = "/admin/preferred_replica_election";

        public static string GetTopicPath(string topic)
        {
            return BrokerTopicsPath + "/" + topic;
        }

        public static string GetTopicPartitionsPath(string topic)
        {
            return GetTopicPath(topic) + "/partitions";
        }

        public static string GetTopicConfigPath(string topic)
        {
            return TopicConfigPath + "/" + topic;
        }

        public static string GetDeleteTopicPath(string topic)
        {
            return DeleteTopicsPath + "/" + topic;
        }

        /* TODO
         *   /*
          * def getController(zkClient: ZkClient): Int = {
     readDataMaybeNull(zkClient, ControllerPath)._1 match {
       case Some(controller) => KafkaController.parseControllerId(controller)
       case None => throw new KafkaException("Controller doesn't exist")
     }
   }*/

         public static string GetTopicPartitionPath(string topic, int partitionId)
         {
             return GetTopicPartitionsPath(topic) + "/" + partitionId;
         }

         public static string GetTopicPartitionLeaderAndIsrPath(string topic, int partitionId)
         {
             return GetTopicPartitionPath(topic, partitionId) + "/" + "state";
         }

         //TODO: public static IList<int> GetSortedBrokerList(ZkClient zkClient)

         //TODO: public static IList<Broker> GetAllBrokersInCluster(ZkClient zkClient)

        public static List<Broker> GetAllBrokersInCluster(ZkClient zkClient)
        {
            var brokerIds = GetChildrenParentMayNotExist(zkClient, BrokerIdsPath).OrderBy(x => x).ToList();
            return
                brokerIds.Select(int.Parse)
                         .Select(id => GetBrokerInfo(zkClient, id))
                         .Where(x => x != null)
                         .ToList();
        }

         //TODO: def getLeaderIsrAndEpochForPartition(zkClient: ZkClient, topic: String, partition: Int):Option[LeaderIsrAndControllerEpoch] = {

         //TODO: def getLeaderAndIsrForPartition(zkClient: ZkClient, topic: String, partition: Int):Option[LeaderAndIsr] = {

         //TODO: def setupCommonPaths(zkClient: ZkClient) {

         //TODO: def parseLeaderAndIsr(leaderAndIsrStr: String, topic: String, partition: Int, stat: Stat)

         //TODO: def getLeaderForPartition(zkClient: ZkClient, topic: String, partition: Int): Option[Int] = {

         //TODO: def getEpochForPartition(zkClient: ZkClient, topic: String, partition: Int): Int = {

         //TODO: def getInSyncReplicasForPartition(zkClient: ZkClient, topic: String, partition: Int): Seq[Int] = {

         //TODO: def getReplicasForPartition(zkClient: ZkClient, topic: String, partition: Int): Seq[Int] = {

         // TODO: def registerBrokerInZk(zkClient: ZkClient, id: Int, host: String, port: Int, timeout: Int, jmxPort: Int) {

        public static string GetConsumerPartitionOwnerPath(string group, string topic, int partition)
        {
            var topicDirs = new ZKGroupTopicDirs(group, topic);
            return topicDirs.ConsumerOwnerDir + "/" + partition;
        }

        //TODO: def leaderAndIsrZkData(leaderAndIsr: LeaderAndIsr, controllerEpoch: Int): String = {

        //TODO: def replicaAssignmentZkData(map: Map[String, Seq[Int]]): String = {

         //TODO: def makeSurePersistentPathExists(client: ZkClient, path: String) {

        private static void CreateParentPath(ZkClient client, string path)
        {
            var parentDir = path.Substring(0, path.LastIndexOf('/'));
            if (parentDir.Length != 0)
            {
                client.CreatePersistent(parentDir, true);
            }
        }

        private static void CreateEphemeralPath(ZkClient client, string path, string data)
        {
            try
            {
                client.CreateEphemeral(path, data);
            }
            catch (ZkNoNodeException e)
            {
                CreateParentPath(client, path);
                client.CreateEphemeral(path, data);
            }
        }

        public static void CreateEphemeralPathExpectConflict(ZkClient client, string path, string data)
        {
            try
            {
                CreateEphemeralPath(client, path, data);
            }
            catch (ZkNodeExistsException e)
            {
                // this can happen when there is connection loss; make sure the data is what we intend to write
                string storedData = null;
                try
                {
                    storedData = ReadData(client, path).Item1;
                }
                catch (ZkNoNodeException e2)
                {
                    // the node disappeared; treat as if node existed and let caller handles this
                }

                if (storedData == null || storedData != data)
                {
                    Logger.InfoFormat("Conflict in {0} data: {1}, stored data: {2}", path, data, storedData);
                    throw e;
                }
                else
                {
                    // otherwise, the creation succeeded, return normally
                    Logger.InfoFormat("{0} exists with value {1} during connection loss", path, data);
                }
                    
            }
        }

        public static void CreateEphemeralPathExpectConflictHandleZKBug(
            ZkClient zkClient,
            string path,
            string data,
            object expectedCallerData,
            Func<string, object, bool> checker,
            int backoffTime)
        {
            while (true)
            {
                try
                {
                    CreateEphemeralPathExpectConflict(zkClient, path, data);
                    return;
                }
                catch (ZkNodeExistsException e)
                {
                    // An ephemeral node may still exist even after its corresponding session has expired
                    // due to a Zookeeper bug, in this case we need to retry writing until the previous node is deleted
                    // and hence the write succeeds without ZkNodeExistsException

                    var writtenData = ZkUtils.ReadDataMaybeNull(zkClient, path).Item1;
                    if (writtenData != null)
                    {
                        if (checker(writtenData, expectedCallerData))
                        {
                            Logger.InfoFormat("I wrote this conflicted ephemeral node [{0}] at {1} a while back in a different session, "
                                              + "hence I will backoff for this node to be deleted by Zookeeper and retry", 
                                              data, path);

                            Thread.Sleep(backoffTime);
                        }
                        else
                        {
                            throw e;
                        }
                    }
                    else
                    {
                        // the node disappeared; retry creating the ephemeral node immediately
                    }
                }
            }
        }

         //TODO: def createPersistentPath(client: ZkClient, path: String, data: String = ""): Unit = {

         //TODO: def createSequentialPersistentPath(client: ZkClient, path: String, data: String = ""): String = {

         //TODO: def updatePersistentPath(client: ZkClient, path: String, data: String) = {

        public static void UpdatePersistentPath(ZkClient client, string path, string data)
        {
            try
            {
                client.WriteData(path, data);
            }
            catch (ZkNoNodeException e)
            {
                CreateParentPath(client, path);
                try
                {
                    client.CreatePersistent(path, data);
                }
                catch (ZkNodeExistsException e2)
                {
                    client.WriteData(path, data);
                }
            }
        }

         //TODO: def conditionalUpdatePersistentPath(client: ZkClient, path: String, data: String, expectVersion: Int): (Boolean, Int) = {

         //TODO: def conditionalUpdatePersistentPathIfExists(client: ZkClient, path: String, data: String, expectVersion: Int): (Boolean, Int) = {

         //TODO: def updateEphemeralPath(client: ZkClient, path: String, data: String): Unit = {

         //TODO: def deletePath(client: ZkClient, path: String): Boolean = {

        public static bool DeletePath(ZkClient client, string path)
        {
            try
            {
                return client.Delete(path);
            }
            catch (ZkNoNodeException e)
            {
                // this can happen during a connection loss event, return normally
                Logger.InfoFormat("{0} deleted during connection loss; This is ok. ", path);
                return false;
            }
            catch (Exception e)
            {
                throw e;
            }
        }



         //TODO: def deletePathRecursive(client: ZkClient, path: String) {

         //TODO: def maybeDeletePath(zkUrl: String, dir: String) {

         public static Tuple<string, Stat> ReadData(ZkClient client, string path)
         {
             var stat = new Stat();
             var dataString = client.ReadData<string>(path, stat);
             return Tuple.Create(dataString, stat);
         }

        public static Tuple<string, Stat> ReadDataMaybeNull(ZkClient client, string path)
        {
            var stat = new Stat();
            try
            {
                var obj = client.ReadData<string>(path, stat);
                return Tuple.Create(obj, stat);
            }
            catch (ZkNoNodeException e)
            {
                return Tuple.Create((string)null, stat);
            }
        }

         //TODO: def getChildren(client: ZkClient, path: String): Seq[String] = {

         //TODO: def getChildrenParentMayNotExist(client: ZkClient, path: String): Seq[String] = {

        public static IList<string> GetChildrenParentMayNotExist(ZkClient client, string path)
        {
            try
            {
                return client.GetChildren(path);
            }
            catch (ZkNoNodeException e)
            {
                return null;
            }
        }


        //TODO: def pathExists(client: ZkClient, path: String): Boolean = {



        public static Cluster GetCluster(ZkClient zkClient)
        {
            var cluster = new Cluster();
            var nodes = GetChildrenParentMayNotExist(zkClient, BrokerIdsPath);
            foreach (var node in nodes)
            {
                var brokerZkString = ReadData(zkClient, BrokerIdsPath + "/" + node).Item1;
                cluster.Add(Broker.CreateBroker(int.Parse(node), brokerZkString));
            }
            return cluster;
        }

         //TODO: def getPartitionLeaderAndIsrForTopics(zkClient: ZkClient, topicAndPartitions: Set[TopicAndPartition])

         //TODO: def getReplicaAssignmentForTopics(zkClient: ZkClient, topics: Seq[String]): mutable.Map[TopicAndPartition, Seq[Int]] = {

        public static IDictionary<string, IDictionary<int, List<int>>> GetPartitionAssignmentForTopics(
            ZkClient zkClient, IList<string> topics)
        {
            IDictionary<string, IDictionary<int, List<int>>> ret = new Dictionary<string, IDictionary<int, List<int>>>();
            foreach (var topic in topics)
            {
                var jsonPartitionMap = ReadDataMaybeNull(zkClient, GetTopicPath(topic)).Item1;
                IDictionary<int, List<int>> partitionMap = new Dictionary<int, List<int>>();
                if (jsonPartitionMap != null)
                {
                    var m = JObject.Parse(jsonPartitionMap);
                    var replicaMap = (IDictionary<string, JToken>) m.Get("partitions");
                    if (replicaMap != null)
                    {
                        partitionMap = replicaMap.ToDictionary(
                            kvp => int.Parse(kvp.Key), kvp => kvp.Value.Values<int>().ToList());
                    }
                }

                Logger.DebugFormat("Partition map for /brokers/topics/{0} is {1}", topic, JObject.FromObject(partitionMap).ToString(Formatting.None));
                ret[topic] = partitionMap;
            }

            return ret;
        }

         //TODO: def getPartitionAssignmentForTopics(zkClient: ZkClient, topics: Seq[String]): mutable.Map[String, collection.Map[Int, Seq[Int]]] = {

         //TODO: def getPartitionsForTopics(zkClient: ZkClient, topics: Seq[String]): mutable.Map[String, Seq[Int]] = {

         //TODO: def getPartitionsBeingReassigned(zkClient: ZkClient): Map[TopicAndPartition, ReassignedPartitionsContext] = {

         //TODO: def parsePartitionReassignmentData(jsonData: String): Map[TopicAndPartition, Seq[Int]] = {

         //TODO: def parseTopicsData(jsonData: String): Seq[String] = {

         //TODO: def getPartitionReassignmentZkData(partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]]): String = {

         //TODO: def updatePartitionReassignmentData(zkClient: ZkClient, partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]]) {

         //TODO: def getPartitionsUndergoingPreferredReplicaElection(zkClient: ZkClient): Set[TopicAndPartition] = {

         //TODO :def deletePartition(zkClient : ZkClient, brokerId: Int, topic: String) {

         //TODO: def getConsumersInGroup(zkClient: ZkClient, group: String): Seq[String] = {

        public static IDictionary<string, List<string>> GetConsumersPerTopic(ZkClient zkClient, string group)
        {
            var dirs = new ZKGroupDirs(group);
            var consumers = GetChildrenParentMayNotExist(zkClient, dirs.ConsumerRegistryDir);
            var consumerPerTopicMap = new Dictionary<string, List<string>>();
            foreach (var consumer in consumers)
            {
                var topicCount = TopicCounts.ConstructTopicCount(group, consumer, zkClient);
                foreach (var topicAndConsumer in topicCount.GetConsumerThreadIdsPerTopic())
                {
                    var topic = topicAndConsumer.Key;
                    var consumerThreadIdSet = topicAndConsumer.Value;
                    foreach (var consumerThreadId in consumerThreadIdSet)
                    {
                        var curConsumers = consumerPerTopicMap.Get(topic);
                        if (curConsumers != null)
                        {
                            curConsumers.Add(consumerThreadId);
                        }
                        else
                        {
                            consumerPerTopicMap[topic] = new List<string> { consumerThreadId };
                        }
                    }
                }
            }

            consumerPerTopicMap = consumerPerTopicMap.ToDictionary(x => x.Key, x => x.Value.OrderBy(y => y).ToList());

            return consumerPerTopicMap;
        }


         //TODO: def getBrokerInfo(zkClient: ZkClient, brokerId: Int): Option[Broker] = {

        public static Broker GetBrokerInfo(ZkClient zkClient, int brokerId)
        {
            var brokerInfo = ReadDataMaybeNull(zkClient, BrokerIdsPath + "/" + brokerId);
            if (brokerInfo != null)
            {
                return Broker.CreateBroker(brokerId, brokerInfo.Item1);
            }
            else
            {
                return null;
            }
        }

         //TODO: def getAllTopics(zkClient: ZkClient): Seq[String] = {

         //TODO: def getAllPartitions(zkClient: ZkClient): Set[TopicAndPartition] = {
       

        //TODO: finish me 
    }
}