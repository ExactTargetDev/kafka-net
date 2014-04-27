namespace Kafka.Client.Network
{
    using System.Reflection;

    using Kafka.Client.Common;

    using log4net;

    /// <summary>
    /// Represents a stateful transfer of Data to or from the network
    /// </summary>
    public abstract class Transmission
    {
        protected static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        protected bool complete;

        protected void ExpectIncomplete()
        {
            if (complete)
            {
                throw new KafkaException("This operation cannot be completed on a complete request.");
            }
        }

        protected void ExpectComplete()
        {
            if (!complete)
            {
                throw new KafkaException("This operation cannot be completed on an incomplete request.");
            }
        }
    }
}