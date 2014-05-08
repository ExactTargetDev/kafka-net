namespace Kafka.Client.Network
{
    using System;
    using System.IO;
    using System.Net.Sockets;
    using System.Reflection;

    using Kafka.Client.Api;

    using log4net;

    /// <summary>
    /// A simple blocking channel with timeouts correctly enabled.
    /// </summary>
    internal class BlockingChannel
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public const int UseDefaultBufferSize = -1;

        public string Host { get; private set; }

        public int Port { get; private set; }

        public int ReadBufferSize { get; private set; }

        public int WriteBufferSize { get; private set; }

        public int ReadTimeoutMs { get; private set; }

        private bool conneted;

        private TcpClient channel;

        private Stream readChannel;

        private Stream writeChannel;

        private readonly object @lock = new object();

        public BlockingChannel(string host, int port, int readBufferSize, int writeBufferSize, int readTimeoutMs)
        {
            this.Host = host;
            this.Port = port;
            this.ReadBufferSize = readBufferSize;
            this.WriteBufferSize = writeBufferSize;
            this.ReadTimeoutMs = readTimeoutMs;
        }

        public void Connect()
        {
            lock (@lock)
            {
                this.channel = new TcpClient(this.Host, this.Port);
                if (this.ReadBufferSize > 0)
                {
                    this.channel.ReceiveBufferSize = this.ReadBufferSize;
                }

                if (this.WriteBufferSize > 0)
                {
                    this.channel.SendBufferSize = this.WriteBufferSize;
                }

                this.channel.ReceiveTimeout = this.ReadTimeoutMs;
                this.channel.NoDelay = true;
                this.channel.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, 1); // TODO: verify this option!

                this.writeChannel = this.channel.GetStream();
                this.readChannel = this.channel.GetStream();
                this.conneted = true;

                // settings may not match what we requested above
                Logger.DebugFormat(
                    "Created socket with SO_TIMEOUT = {0} (requested {1}), SO_RCBBUG = {2} (requested {3}), SO_SNDBUF = {4} (requested {5}).",
                    this.channel.Client.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendTimeout),
                    this.ReadTimeoutMs,
                        this.channel.Client.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveTimeout),
                        this.ReadBufferSize,
                        this.channel.Client.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveBuffer),
                        this.WriteBufferSize); // TODO: verify
            }
        }

        public void Disconnect()
        {
            lock (@lock)
            {
                if (this.conneted || this.channel != null)
                {
                    // closing the main socket channel *should* close the read channel
                    // but let's do it to be sure.
                    try
                    {
                        this.channel.Close();
                    }
                    catch (Exception e)
                    {
                        Logger.Warn(e.Message, e);
                    }

                    this.channel = null;
                    this.readChannel = null;
                    this.writeChannel = null;
                    this.conneted = false;
                }
            }
        }

        public bool IsConnected
        {
            get
            {
                return this.conneted;
            }
        }

        public int Send(RequestOrResponse request)
        {
            if (!this.conneted)
            {
                throw new IOException("Channel is closed!");
            }

            var send = new BoundedByteBufferSend(request);
            return send.WriteCompletely(this.writeChannel);
        }

        public Receive Receive()
        {
            if (!this.conneted)
            {
                throw new IOException("Channel is closed!");
            }

            var response = new BoundedByteBufferReceive();
            response.ReadCompletely(this.readChannel);

            return response;
        }
    }
}