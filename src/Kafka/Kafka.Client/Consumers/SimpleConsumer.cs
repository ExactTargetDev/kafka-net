namespace Kafka.Client.Consumers
{
    using System;
    using System.Reflection;

    using Kafka.Client.Api;
    using Kafka.Client.Cfg;
    using Kafka.Client.Network;

    using log4net;

    public class SimpleConsumer : IDisposable
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

        // TODO: private val fetchRequestAndResponseStats = FetchRequestAndResponseStatsRegistry.getFetchRequestAndResponseStats(clientId)

        private bool isClosed = false;

        public SimpleConsumer(string host, int port, int soTimeout, int bufferSize, string clientId)
        {
            this.Host = host;
            this.Port = port;
            this.SoTimeout = soTimeout;
            this.BufferSize = bufferSize;
            this.ClientId = clientId;
            // TODO: ConsumerConfig.validateClientId(clientId)
            this.blockingChannel = new BlockingChannel(Host, Port, BufferSize, BlockingChannel.UseDefaultBufferSize, SoTimeout);
        }

        private BlockingChannel Connect()
        {
            this.Dispose();
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

        public void Dispose()
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

        //TODO: fetch offsets

        //TODO: def getOffsetsBefore(request: OffsetRequest) = OffsetResponse.readFrom(sendRequest(request).buffer)

        //TODO: def commitOffsets(request: OffsetCommitRequest) = OffsetCommitResponse.readFrom(sendRequest(request).buffer)

        //TODO: def fetchOffsets(request: OffsetFetchRequest) = OffsetFetchResponse.readFrom(sendRequest(request).buffer)

        private void GetOrMakeConnection()
        {
            if (!this.isClosed && !this.blockingChannel.IsConnected)
            {
                this.Connect();
            }
        }

        //TODO:  def earliestOrLatestOffset(topicAndPartition: TopicAndPartition, earliestOrLatest: Long, consumerId: Int): Long = {

    }
}