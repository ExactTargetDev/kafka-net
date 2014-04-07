namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    using Kafka.Client.Common;
    using Kafka.Client.Messages;

    using System.Linq;

    using Kafka.Client.Extensions;

    internal class ProducerRequest : RequestOrResponse 
    {
        public const short CurrentVersion = 0;

        public short VersionId { get; private set; }

        public string ClientId { get; private set; }

        public short RequiredAcks { get; private set; }

        public int AckTimeoutMs { get; private set; }

        public IDictionary<TopicAndPartition, ByteBufferMessageSet> Data { get; set; }

        public ProducerRequest(
            int correlationId,
            string clientId,
            short requiredAcks,
            int ackTimeoutMs,
            IDictionary<TopicAndPartition, ByteBufferMessageSet> data) : base(RequestKeys.ProduceKey, correlationId)
        {
            this.VersionId = CurrentVersion;
            this.CorrelationId = correlationId;
            this.ClientId = clientId;
            this.RequiredAcks = requiredAcks;
            this.AckTimeoutMs = ackTimeoutMs;
            this.Data = data;

            //this.dataGroupedByTopic = //TODO:private lazy val dataGroupedByTopic = data.groupBy(_._1.topic)
            //this.topicPartitionMessageSizeMap = //TODO: data.map(r => r._1 -> r._2.sizeInBytes).toMap
            this.numPartitions = data.Count();
        }

        private Lazy<Dictionary<string, Dictionary<TopicAndPartition, ByteBufferMessageSet>>> dataGroupedByTopic;

        private Dictionary<TopicAndPartition, int> topicPartitionMessageSizeMap;

        private int numPartitions;

        public override void WriteTo(MemoryStream buffer)
        {
            buffer.PutShort(this.VersionId);
            buffer.PutInt(this.CorrelationId);
            ApiUtils.WriteShortString(buffer, this.ClientId);
            buffer.PutShort(this.RequiredAcks);
            buffer.PutInt(this.AckTimeoutMs);

            /*
             * TODO
             *  //save the topic structure
    buffer.putInt(dataGroupedByTopic.size) //the number of topics
    dataGroupedByTopic.foreach {
      case (topic, topicAndPartitionData) =>
        writeShortString(buffer, topic) //write the topic
        buffer.putInt(topicAndPartitionData.size) //the number of partitions
        topicAndPartitionData.foreach(partitionAndData => {
          val partition = partitionAndData._1.partition
          val partitionMessageData = partitionAndData._2
          val bytes = partitionMessageData.buffer
          buffer.putInt(partition)
          buffer.putInt(bytes.limit)
          buffer.put(bytes)
          bytes.rewind
        })
    }*/
        }

        public override int SizeInBytes
        {
            get
            {
                throw new NotImplementedException(); //TODO:
            }
        }

        public override string ToString()
        {
            return this.Describe(true);
        }

        //TODO: override  def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.RequestChannelRequest): Unit = {

        public override string Describe(bool details)
        {
            throw new NotImplementedException();
        }

        public void EmptyData()
        {
            this.Data.Clear();
        }

    }
}