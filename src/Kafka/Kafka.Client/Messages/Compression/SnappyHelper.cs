namespace Kafka.Client.Messages.Compression
{
    using System;
    using System.IO;

    public static class SnappyHelper
    {
        public static byte[] Compress(byte[] input)
        {
            var compressionArray = new byte[input.Length + 2];
            var compressedLength = SnappyCompress.Compress(ByteBuffer.NewSync(compressionArray), ByteBuffer.NewSync(input));

            var output = new byte[compressedLength];

            Array.Copy(compressionArray, output, compressedLength);

            return output;
        }

        public static byte[] Decompress(byte[] input)
        {
            var buffer = SnappyDecompress.Decompress(ByteBuffer.NewSync(input));
            return buffer.ToByteArray();
        }
    }
}
