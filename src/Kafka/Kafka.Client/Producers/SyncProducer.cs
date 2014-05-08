namespace Kafka.Client.Producers
{
    using System;
    using System.IO;
    using System.Reflection;

    using Kafka.Client.Api;
    using Kafka.Client.Network;

    using log4net;

    internal class SyncProducer : IDisposable
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public const short RequestKey = 0;

        public readonly Random RandomGenerator = new Random();

        private readonly object @lock = new object();

        private bool shutdown;

        private readonly BlockingChannel blockingChannel;

        public string BrokerInfo { get; private set; }

        public SyncProducerConfig Config { get; private set; }

        private readonly ProducerRequestStats producerRequestStats;

        public SyncProducer(SyncProducerConfig config)
        {
            Logger.Debug("Instantiating Scala Sync Producer");

            this.Config = config;
            this.blockingChannel = new BlockingChannel(config.Host, config.Port, BlockingChannel.UseDefaultBufferSize, config.SendBufferBytes, config.RequestTimeoutMs);
            this.BrokerInfo = string.Format("host_{0}-port_{1}", config.Host, config.Port);
            this.producerRequestStats = ProducerRequestStatsRegistry.GetProducerRequestStats(config.ClientId);
        }

        private void VerifyRequest(RequestOrResponse request)
        {
            /**
             * This seems a little convoluted, but the idea is to turn on verification simply changing log4j settings
             * Also, when verification is turned on, care should be taken to see that the logs don't fill up with unnecessary
             * Data. So, leaving the rest of the logging at TRACE, while errors should be logged at ERROR level
             */
            if (Logger.IsDebugEnabled)
            {
                var buffer = new BoundedByteBufferSend(request).Buffer;
                Logger.Debug("Verifying sendbuffer of size " + buffer.Limit());
                var requestTypeId = buffer.GetShort();
                if (requestTypeId == RequestKeys.ProduceKey)
                {
                    var innerRequest = ProducerRequest.ReadFrom(buffer);
                    Logger.Debug(innerRequest.ToString());
                }
            }
        }

        public Receive DoSend(RequestOrResponse request, bool readResponse = true)
        {
            lock (@lock)
            {
                this.VerifyRequest(request);
                this.GetOrMakeConnection();

                Receive response = null;
                try
                {
                    this.blockingChannel.Send(request);
                    if (readResponse)
                    {
                        response = this.blockingChannel.Receive();
                    }
                    else
                    {
                        Logger.Debug("Skipping reading response");
                    }
                }
                catch (IOException)
                {
                    // no way to tell if write succeeded. Disconnect and re-throw exception to let client handle retry
                    this.Disconnect();
                    throw;
                }

                return response;
            }
        }

        public ProducerResponse Send(ProducerRequest producerRequest)
        {
            var requestSize = producerRequest.SizeInBytes;
            this.producerRequestStats.GetProducerRequestStats(this.BrokerInfo).RequestSizeHist.Update(requestSize);
            this.producerRequestStats.GetProducerRequestAllBrokersStats().RequestSizeHist.Update(requestSize);

            Receive response = null;
            var specificTimer = this.producerRequestStats.GetProducerRequestStats(this.BrokerInfo).RequestTimer;
            var aggregateTimer = this.producerRequestStats.GetProducerRequestAllBrokersStats().RequestTimer;

            aggregateTimer.Time(() => specificTimer.Time(() =>
                {
                    response = this.DoSend(producerRequest, producerRequest.RequiredAcks != 0);
                }));

            if (producerRequest.RequiredAcks != 0)
            {
                return ProducerResponse.ReadFrom(response.Buffer);
            }
            else
            {
                return null;
            }
        }

        public TopicMetadataResponse Send(TopicMetadataRequest request)
        {
            var response = this.DoSend(request);
            return TopicMetadataResponse.ReadFrom(response.Buffer);
        }

        public void Dispose()
        {
            lock (@lock)
            {
                this.Disconnect();
                this.shutdown = true;
            }
        }

        /// <summary>
        /// Disconnect from current channel, closing connection.
        /// Side effect: channel field is set to null on successful disconnect
        /// </summary>
        private void Disconnect()
        {
            try
            {
                if (this.blockingChannel.IsConnected)
                {
                    Logger.InfoFormat("Disconnecting from {0}:{1}", this.Config.Host, this.Config.Port);
                    this.blockingChannel.Disconnect();
                }
            } 
            catch (Exception e) 
            {
                Logger.Error("Error on disconnect", e);
            }
        }

        private BlockingChannel Connect()
        {
            if (!this.blockingChannel.IsConnected && !this.shutdown)
            {
                try
                {
                    this.blockingChannel.Connect();
                    Logger.InfoFormat("Connected to {0}:{1} for producing", this.Config.Host, this.Config.Port);
                }
                catch (Exception e)
                {
                    this.Disconnect();
                    Logger.Error(string.Format("Producer connection to {0}:{1} unsuccessful", this.Config.Host, this.Config.Port), e);
                    throw;
                }
            }

            return this.blockingChannel;
        }

        private void GetOrMakeConnection()
        {
            if (!this.blockingChannel.IsConnected)
            {
                this.Connect();
            }
        }
    }
}