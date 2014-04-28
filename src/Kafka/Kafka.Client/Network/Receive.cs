namespace Kafka.Client.Network
{
    using System.IO;

    using Kafka.Client.Common.Imported;

    /// <summary>
    /// A transmission that is being received from a channel
    /// </summary>
    public abstract class Receive : Transmission
    {
        public abstract ByteBuffer Buffer { get; }

        public abstract int ReadFrom(Stream channel);

        public int ReadCompletely(Stream channel)
        {
            var totalRead = 0;
            while (!this.complete)
            {
                var read = this.ReadFrom(channel);
                Logger.DebugFormat("{0} bytes read", read);
                
                totalRead += read;
            }

            return totalRead;
        }
    }
}