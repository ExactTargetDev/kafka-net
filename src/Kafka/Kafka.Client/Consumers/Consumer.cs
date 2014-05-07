namespace Kafka.Client.Consumers
{
    using Kafka.Client.Cfg;

    public static class Consumer
    {
        /// <summary>
        /// Create a ConsumerConnector
        /// </summary>
        /// <param name="config">at the minimum, need to specify the groupid of the consumer and the zookeeper connection string zookeeper.connect.</param>
        /// <returns></returns>
         public static IConsumerConnector Create(ConsumerConfig config)
         {
             var consumeConnect = new ZookeeperConsumerConnector(config);
             return consumeConnect;
         }
    }
}