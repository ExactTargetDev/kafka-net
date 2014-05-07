namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    using Kafka.Client.Api;
    using Kafka.Client.Cfg;
    using Kafka.Client.Common;
    using Kafka.Client.Network;

    using log4net;

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
                return string.Format("host_{0}-port_{1}", Host, Port);
            }
        }

        private FetchRequestAndResponseStats fetchRequestAndResponseStats;

        private bool isClosed = false;

        public SimpleConsumer(string host, int port, int soTimeout, int bufferSize, string clientId)
        {
            this.Host = host;
            this.Port = port;
            this.SoTimeout = soTimeout;
            this.BufferSize = bufferSize;
            this.ClientId = clientId;
            ConsumerConfig.ValidateClientId(clientId);
            this.blockingChannel = new BlockingChannel(Host, Port, BufferSize, BlockingChannel.UseDefaultBufferSize, SoTimeout);
            this.fetchRequestAndResponseStats =
                FetchRequestAndResponseStatsRegistry.GetFetchRequestAndResponseStats(clientId);
        }

        private BlockingChannel Connect()
        {
            this.Close();
            this.blockingChannel.Connect();
            return blockingChannel;
        }

        private void Disconnect()
        {
            if (blockingChannel.IsConnected)
            {
                Logger.DebugFormat("Disconnecting from {0}:{1}", Host, Port);
                blockingChannel.Disconnect();
            }
        }

        private void Reconnect()
        {
            this.Disconnect();
            this.Connect();
        }

        public void Close()
        {
            lock (@lock)
            {
                this.Disconnect();
                isClosed = true;
            }
        }

        private Receive SendRequest(RequestOrResponse request)
        {
            lock(@lock)
            {
                this.GetOrMakeConnection();
                Receive response = null;
                try
                {
                    this.blockingChannel.Send(request);
                    response = this.blockingChannel.Receive();
                }
                catch (Exception e)
                {
                    Logger.WarnFormat("Reconnect due to socket error {0}",e.Message);

                    // retry once
                    try
                    {
                        this.Reconnect();
                        blockingChannel.Send(request);
                        response = blockingChannel.Receive();
                    }
                    catch (Exception ee)
                    {
                        this.Disconnect();
                        throw ee;
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
            var specificTimer = fetchRequestAndResponseStats.GetFetchRequestAndResponseStats(BrokerInfo).RequestTimer;
            var aggregateTimer = fetchRequestAndResponseStats.GetFetchRequestAndResponseAllBrokersStats().RequestTimer;
            aggregateTimer.Time(() => specificTimer.Time(() =>
                { response = this.SendRequest(request); }));

            var fetchResponse = FetchResponse.ReadFrom(response.Buffer);
            var fetchedSize = fetchResponse.SizeInBytes;

            fetchRequestAndResponseStats.GetFetchRequestAndResponseStats(BrokerInfo).RequestSizeHist.Update(fetchedSize);
            fetchRequestAndResponseStats.GetFetchRequestAndResponseAllBrokersStats().RequestSizeHist.Update(fetchedSize);

            return fetchResponse;
        }

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

        public long EarliestOrLatestOffset(TopicAndPartition topicAndPartition, long earliestOrLatest, int consumerId)
        {
            var request =
                new OffsetRequest(
                    requestInfo:
                        new Dictionary<TopicAndPartition, PartitionOffsetRequestInfo>
                            {
                                {
                                    topicAndPartition,
                                    new PartitionOffsetRequestInfo(earliestOrLatest, 1)
                                }
                            },
                    clientId: this.ClientId,
                    replicaId: consumerId);
            var partitionErrorAndOffset = GetOffsetsBefore(request).PartitionErrorAndOffsets[topicAndPartition];
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