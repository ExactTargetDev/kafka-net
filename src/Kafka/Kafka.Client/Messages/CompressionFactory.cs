namespace Kafka.Client.Messages
{
    using System.IO;
    using System.IO.Compression;

    using Kafka.Client.Common;

    using Snappy;

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
                     return new SnappyStream(stream, CompressionMode.Compress);
                 default:
                     throw new UnknownCodecException("Unknown codec " + compressionCodec);
             }
         }

        public static Stream BuildReader(CompressionCodecs compressionCodec, Stream stream)
        {
            switch (compressionCodec)
            {
                case CompressionCodecs.DefaultCompressionCodec:
                    return new GZipStream(stream, CompressionMode.Decompress);
                case CompressionCodecs.GZIPCompressionCodec:
                    return new GZipStream(stream, CompressionMode.Decompress);
                case CompressionCodecs.SnappyCompressionCodec:
                    return new SnappyStream(stream, CompressionMode.Decompress);
                default:
                    throw new UnknownCodecException("Unknown codec " + compressionCodec);
            }
        }
    }
}