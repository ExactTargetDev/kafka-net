namespace Kafka.Client.Api
{
    using System;
    using System.IO;
    using System.Reflection;

    using Kafka.Client.Network;

    using log4net;

    public abstract class RequestOrResponse
    {
        public short? RequestId { get; protected set; }

        public int CorrelationId { get; protected set; }

        protected RequestOrResponse(short? requestId, int correlationId)
        {
            this.RequestId = requestId;
            this.CorrelationId = correlationId;
        }

        protected static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public abstract int SizeInBytes { get; }

        public abstract void WriteTo(MemoryStream bufffer);

        public virtual void HandleError(Exception e, RequestChannel requestChannel, RequestChannelRequest request)
        {
            throw new NotImplementedException("Handle error is not used in client");
        }

        /// <summary>
        /// The purpose of this API is to return a string description of the Request mainly for the purpose of request logging
        /// This API has no meaning for a Response object.
        /// </summary>
        /// <param name="details">If this is false, omit the parts of the request description that are proportional to the number of topics or partitions. This is mainly to control the amount of request logging. </param>
        /// <returns></returns>
        public abstract string Describe(bool details);
    }
}