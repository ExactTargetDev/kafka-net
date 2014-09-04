using Kafka.Client.Cfg;

namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    using Kafka.Client.Api;
    using Kafka.Client.Common;
    using Kafka.Client.Network;

    using log4net;

    /// <summary>
    /// A consumer of kafka messages
    /// </summary>
    public class SimpleConsumer
    {
        public string Host { get; private set; }

        public int Port { get; private set; }

        public int SoTimeout { get; private set; }

        public int BufferSize { get; private set; }

        public string ClientId { get; private set; }

        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly object @lock = new object();

        private readonly BlockingChannel blockingChannel;

        public string BrokerInfo
        {
            get
            {
                return string.Format("host_{0}-port_{1}", this.Host, this.Port);
            }
        }

        private readonly FetchRequestAndResponseStats fetchRequestAndResponseStats;

        private bool isClosed;

        public SimpleConsumer(string host, int port, int soTimeout, int bufferSize, string clientId)
        {
            this.Host = host;
            this.Port = port;
            this.SoTimeout = soTimeout;
            this.BufferSize = bufferSize;
            this.ClientId = clientId;
            ConsumerConfig.ValidateClientId(clientId);
            this.blockingChannel = new BlockingChannel(this.Host, this.Port, this.BufferSize, BlockingChannel.UseDefaultBufferSize, this.SoTimeout);
            this.fetchRequestAndResponseStats =
                FetchRequestAndResponseStatsRegistry.GetFetchRequestAndResponseStats(clientId);
        }

// ReSharper disable UnusedMethodReturnValue.Local
        private BlockingChannel Connect()
// ReSharper restore UnusedMethodReturnValue.Local
        {
            this.Close();
            this.blockingChannel.Connect();
            return this.blockingChannel;
        }

        private void Disconnect()
        {
            if (this.blockingChannel.IsConnected)
            {
                Logger.DebugFormat("Disconnecting from {0}:{1}", this.Host, this.Port);
                this.blockingChannel.Disconnect();
            }
        }

        private void Reconnect()
        {
            this.Disconnect();
            this.Connect();
        }

        //TODO: dispose!
        public void Close()
        {
            lock (@lock)
            {
                this.Disconnect();
                this.isClosed = true;
            }
        }

        private Receive SendRequest(RequestOrResponse request)
        {
            lock (@lock)
            {
                this.GetOrMakeConnection();
                Receive response;
                try
                {
                    this.blockingChannel.Send(request);
                    response = this.blockingChannel.Receive();
                }
                catch (Exception e)
                {
                    Logger.WarnFormat("Reconnect due to socket error {0}", e.Message);

                    // retry once
                    try
                    {
                        this.Reconnect();
                        this.blockingChannel.Send(request);
                        response = this.blockingChannel.Receive();
                    }
                    catch (Exception)
                    {
                        this.Disconnect();
                        throw;
                    }
                }

                return response;
            }
        }

        public TopicMetadataResponse Send(TopicMetadataRequest request)
        {
            var response = this.SendRequest(request);
            return TopicMetadataResponse.ReadFrom(response.Buffer);
        }

        /// <summary>
        /// Fetch a set of messages from a topic.
        /// </summary>
        /// <param name="request"> specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.</param>
        /// <returns></returns>
        internal FetchResponse Fetch(FetchRequest request)
        {
            Receive response = null;
            if (StatSettings.ConsumerStatsEnabled)
            {
                var specificTimer =
                    this.fetchRequestAndResponseStats.GetFetchRequestAndResponseStats(this.BrokerInfo).RequestTimer;
                var aggregateTimer =
                    this.fetchRequestAndResponseStats.GetFetchRequestAndResponseAllBrokersStats().RequestTimer;
                aggregateTimer.Time(() => specificTimer.Time(() =>
                {
                    response = this.SendRequest(request);

                }));
            }
            else
            {
                response = this.SendRequest(request);
            }

            var fetchResponse = FetchResponse.ReadFrom(response.Buffer);
            var fetchedSize = fetchResponse.SizeInBytes;

            this.fetchRequestAndResponseStats.GetFetchRequestAndResponseStats(this.BrokerInfo).RequestSizeHist.Update(fetchedSize);
            this.fetchRequestAndResponseStats.GetFetchRequestAndResponseAllBrokersStats().RequestSizeHist.Update(fetchedSize);

            return fetchResponse;
        }

        /// <summary>
        /// Get a list of valid offsets (up to maxSize) before the given time.
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        internal OffsetResponse GetOffsetsBefore(OffsetRequest request)
        {
            return OffsetResponse.ReadFrom(this.SendRequest(request).Buffer);
        }

        private void GetOrMakeConnection()
        {
            if (!this.isClosed && !this.blockingChannel.IsConnected)
            {
                this.Connect();
            }
        }

        /// <summary>
        /// Get the earliest or latest offset of a given topic, partition.
        /// </summary>
        /// <param name="topicAndPartition">Topic and partition of which the offset is needed.</param>
        /// <param name="earliestOrLatest">A value to indicate earliest or latest offset.</param>
        /// <param name="consumerId">Id of the consumer which could be a consumer client, SimpleConsumerShell or a follower broker.</param>
        /// <returns>Requested offset.</returns>
        public long EarliestOrLatestOffset(TopicAndPartition topicAndPartition, long earliestOrLatest, int consumerId)
        {
            var request =
                new OffsetRequest(
                        new Dictionary<TopicAndPartition, PartitionOffsetRequestInfo>
                            {
                                {
                                    topicAndPartition,
                                    new PartitionOffsetRequestInfo(earliestOrLatest, 1)
                                }
                            },
                    clientId: this.ClientId,
                    replicaId: consumerId);
            var partitionErrorAndOffset = this.GetOffsetsBefore(request).PartitionErrorAndOffsets[topicAndPartition];
            long offset;
            if (partitionErrorAndOffset.Error == ErrorMapping.NoError)
            {
                offset = partitionErrorAndOffset.Offsets[0];
            }
            else
            {
                throw ErrorMapping.ExceptionFor(partitionErrorAndOffset.Error);
            }

            return offset;
        }
    }
}