namespace Kafka.Client.Api
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    using Kafka.Client.Network;
    using Kafka.Client.Serializers;

    using Kafka.Client.Extensions;

    using System.Linq;

    public class TopicMetadataRequest : RequestOrResponse
    {
        public const short CurrentVersion = 0;

        public const string DefaultClientId = "";

        public static TopicMetadataRequest ReadFrom(MemoryStream buffer)
        {
            throw new NotImplementedException();
        }

        public short VersionId { get; private set; }

        public string ClientId { get; private set; }

        public IList<string> Topics { get; private set; }

        public TopicMetadataRequest(short versionId, int correlationId, string clientId, IList<string> topics)
            : base(RequestKeys.MetadataKey, correlationId)
        {
            this.VersionId = versionId;
            this.ClientId = clientId;
            this.Topics = topics;
        }

        public TopicMetadataRequest(IList<string> topics, int correlationId) : this(CurrentVersion, correlationId, DefaultClientId, topics)
        {
        }

        public override void WriteTo(MemoryStream buffer)
        {
            buffer.PutShort(VersionId);
            buffer.PutInt(CorrelationId);
            ApiUtils.WriteShortString(buffer, ClientId);
            buffer.PutInt(Topics.Count);
            foreach (var topic in Topics)
            {
                ApiUtils.WriteShortString(buffer, topic);
            }
        }

        public override int SizeInBytes
        {
            get
            {
                return 2 + /* version id */ 
                    4 + /* correlation id */ ApiUtils.ShortStringLength(ClientId)
                       + /* client id */ 4
                       + /* number of topics */ Topics.Aggregate(0, (i, s) => i + ApiUtils.ShortStringLength(s));
                    /* topics */
            }
        }

        public override string ToString()
        {
            return this.Describe(true);
        }

        public override void HandleError(Exception e, RequestChannel requestChannel, RequestChannelRequest request)
        {
            base.HandleError(e, requestChannel, request);
            //TODO 
            /*
             * val topicMetadata = topics.map {
      topic => TopicMetadata(topic, Nil, ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
    }
    val errorResponse = TopicMetadataResponse(topicMetadata, correlationId)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)))*/
        }

        public override string Describe(bool details)
        {
            throw new NotImplementedException();
        }
    }
}