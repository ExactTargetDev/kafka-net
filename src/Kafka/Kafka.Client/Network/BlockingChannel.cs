namespace Kafka.Client.Network
{
    using System;
    using System.IO;
    using System.Net.Sockets;
    using System.Reflection;

    using Kafka.Client.Api;

    using log4net;

    public class BlockingChannel
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public const int UseDefaultBufferSize = -1;

        public string Host { get; private set; }

        public int Port { get; private set; }

        public int ReadBufferSize { get; private set; }

        public int WriteBufferSize { get; private set; }

        public int ReadTimeoutMs { get; private set; }

        private bool conneted = false;

        private TcpClient channel = null;

        private Stream readChannel = null;

        private Stream writeChannel = null;

        private object @lock = new object();

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
                if (ReadBufferSize > 0)
                {
                    this.channel.ReceiveBufferSize = ReadBufferSize;
                }
                if (WriteBufferSize > 0)
                {
                    this.channel.SendBufferSize = WriteBufferSize;
                }
                channel.ReceiveTimeout = ReadTimeoutMs;
                channel.NoDelay = true;
                //TODO: channel.configureBlocking(true)
                //TODO: channel.socket.setKeepAlive(true)

                this.writeChannel = channel.GetStream();
                this.readChannel = channel.GetStream();
                this.conneted = true;

                /*TODO 
                 * // settings may not match what we requested above
      val msg = "Created socket with SO_TIMEOUT = %d (requested %d), SO_RCVBUF = %d (requested %d), SO_SNDBUF = %d (requested %d)."
      debug(msg.format(channel.socket.getSoTimeout,
                       readTimeoutMs,
                       channel.socket.getReceiveBufferSize, 
                       readBufferSize,
                       channel.socket.getSendBufferSize,
                       writeBufferSize))*/
            }
        }

        public void Disconnect()
        {
            lock (@lock)
            {
                if (conneted || channel != null)
                {
                    // closing the main socket channel *should* close the read channel
                    // but let's do it to be sure.
                    try
                    {
                        channel.Close();
                    }
                    catch (Exception e)
                    {
                        Logger.Warn(e.Message, e);
                    }

                    channel = null;
                    readChannel = null;
                    writeChannel = null;
                    conneted = false;
                }
            }
        }

        public bool IsConnected
        {
            get
            {
                return conneted;
            }
        }

        public int Send(RequestOrResponse request)
        {
            if (!conneted)
            {
                throw new IOException("Channel is closed!");
            }

            var send = new BoundedByteBufferSend(request);
            return send.WriteCompletely(writeChannel);
        }

        public Receive Receive()
        {
            if (!conneted)
            {
                throw new IOException("Channel is closed!");
            }

            var response = new BoundedByteBufferReceive();
            response.ReadCompletely(readChannel);

            return response;
        }

    }
}