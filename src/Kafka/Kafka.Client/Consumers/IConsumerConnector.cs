namespace Kafka.Client.Consumers
{
    using System.Collections.Generic;

    using Kafka.Client.Serializers;

    /// <summary>
    /// Main interface for consumer
    /// </summary>
    public interface IConsumerConnector
    {
        /// <summary>
        /// Create a list of MessageStreams for each topic.
        /// </summary>
        /// <param name="topicCountMap">a map of (topic, #streams) pair</param>
        /// <returns>a map of (topic, list of  KafkaStream) pairs. The number of items in the list is #streams. Each stream supports an iterator over message/metadata pairs.</returns>
        IDictionary<string, IList<KafkaStream<byte[], byte[]>>> CreateMessageStreams(
            IDictionary<string, int> topicCountMap);

        /// <summary>
        /// Create a list of MessageStreams for each topic.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="topicCountMap">a map of (topic, #streams) pair</param>
        /// <param name="keyDecoder">Decoder to decode the key portion of the message</param>
        /// <param name="valueDecoder">Decoder to decode the value portion of the message</param>
        /// <returns>a map of (topic, list of  KafkaStream) pairs. The number of items in the list is #streams. Each stream supports an iterator over message/metadata pairs.</returns>
        IDictionary<string, IList<KafkaStream<TKey, TValue>>> CreateMessageStreams<TKey, TValue>(IDictionary<string, int> topicCountMap, IDecoder<TKey> keyDecoder, IDecoder<TValue> valueDecoder);

        /// <summary>
        /// Create a list of message streams for all topics that match a given filter.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="topicFilter"> Either a Whitelist or Blacklist TopicFilter object.</param>
        /// <param name="numStreams">Number of streams to return</param>
        /// <param name="keyDecoder">Decoder to decode the key portion of the message</param>
        /// <param name="valueDecoder">Decoder to decode the value portion of the message</param>
        /// <returns>a list of KafkaStream each of which provides an iterator over message/metadata pairs over allowed topics.</returns>
        IList<KafkaStream<TKey, TValue>> CreateMessageStreamsByFilter<TKey, TValue>(
            TopicFilter topicFilter,
            int numStreams = 1,
            IDecoder<TKey> keyDecoder = null,
            IDecoder<TValue> valueDecoder = null);

        /// <summary>
        /// Commit the offsets of all broker partitions connected by this connector.
        /// </summary>
        void CommitOffsets();

        /// <summary>
        /// Shut down the connector
        /// </summary>
        void Shutdown();
    }

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