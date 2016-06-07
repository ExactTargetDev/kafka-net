namespace Kafka.Client.Network
{
    using System.IO;
    using System.Reflection;

    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;

    using log4net;

    /// <summary>
    /// Represents a stateful transfer of Data to or from the network
    /// </summary>
    internal abstract class Transmission
    {
        protected static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        protected bool complete;

        protected void ExpectIncomplete()
        {
            if (this.complete)
            {
                throw new KafkaException("This operation cannot be completed on a complete request.");
            }
        }

        protected void ExpectComplete()
        {
            if (!this.complete)
            {
                throw new KafkaException("This operation cannot be completed on an incomplete request.");
            }
        }
    }

    /// <summary>
    /// A transmission that is being received from a channel
    /// </summary>
    internal abstract class Receive : Transmission
    {
        public abstract ByteBuffer Buffer { get; }

        public abstract int ReadFrom(Stream channel);

        public int ReadCompletely(Stream channel)
        {
            var totalRead = 0;
			var iter = 0;
            while (!this.complete)
            {
                var read = this.ReadFrom(channel);
                Logger.DebugFormat("{0} bytes read", read);

                totalRead += read;
				iter++;
				if(iter > 1 && totalRead == 0)
				{
					throw new InvalidRequestException("Reading 0 bytes");
				}
            }

            return totalRead;
        }
    }

    /// <summary>
    /// A transmission that is being sent out to the channel
    /// </summary>
    internal abstract class Send : Transmission
    {
        public abstract int WriteTo(Stream channel);

        public int WriteCompletely(Stream channel)
        {
            var totalWritten = 0;
            while (!this.complete)
            {
                var written = this.WriteTo(channel);
                Logger.DebugFormat("{0} bytes written", written);
                totalWritten += written;
            }

            return totalWritten;
        }
    }
}