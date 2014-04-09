namespace Kafka.Client.Messages
{
    using System.IO;
    using System.IO.Compression;

    using Kafka.Client.Common;

    public class CompressionFactory
    {
         public static Stream BuildWriter(CompressionCodecs compressionCodec, Stream stream)
         {
             switch (compressionCodec)
             {
                 case CompressionCodecs.DefaultCompressionCodec:
                     return new GZipStream(stream, CompressionMode.Compress, true);
                 case CompressionCodecs.GZIPCompressionCodec:
                     return new GZipStream(stream, CompressionMode.Compress, true);
                 case CompressionCodecs.SnappyCompressionCodec:
                     //return new SnappyStreeam(stream); //TODO:
                 default:
                     throw new UnknownCodecException("Unknown codec " + compressionCodec);

             }
         }

        public static Stream BuildReader(CompressionCodecs compressionCodec, Stream stream)
        {
            switch (compressionCodec)
            {
                case  CompressionCodecs.DefaultCompressionCodec:
                    return new GZipStream(stream, CompressionMode.Decompress);
                case CompressionCodecs.GZIPCompressionCodec:
                    return new GZipStream(stream, CompressionMode.Decompress);
                case CompressionCodecs.SnappyCompressionCodec:
                    //return new SnappyStream(stream); //TODO:
                default:
                    throw new UnknownCodecException("Unknown codec " + compressionCodec);
            }
        }
    }
}