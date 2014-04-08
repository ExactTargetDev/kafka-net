namespace Kafka.Client.Network
{
    using System.IO;

    /// <summary>
    /// A transmission that is being sent out to the channel
    /// </summary>
    public abstract class Send : Transmission
    {
        public abstract int WriteTo(Stream channel);

        public int WriteCompletely(Stream channel)
        {
            var totalWritten = 0;
            while (!complete)
            {
                var written = this.WriteTo(channel);
                Logger.DebugFormat("{0} bytes written", written);
                totalWritten += written;
            }

            return totalWritten;
        }
    }
}