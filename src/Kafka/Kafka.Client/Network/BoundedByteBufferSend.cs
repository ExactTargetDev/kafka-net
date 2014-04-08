namespace Kafka.Client.Network
{
    using System;
    using System.IO;

    using Kafka.Client.Api;
    using Kafka.Client.Extensions;

    public class BoundedByteBufferSend : Send
    {
        public MemoryStream Buffer { get; private set; }

        private MemoryStream sizeBuffer = new MemoryStream(4);

        public BoundedByteBufferSend(MemoryStream buffer)
        {
            this.Buffer = buffer;

            if (buffer.Length > int.MaxValue - sizeBuffer.Length)
            {
                throw new ArgumentException("Attempt to create a bounded buffer of " + buffer.Length + "bytes, but the maximum allowable size for a bounded buffer is " + (int.MaxValue - sizeBuffer.Length) );
            }
            this.sizeBuffer.PutInt((int)buffer.Capacity);
            this.sizeBuffer.Position = 0;
        }

        public BoundedByteBufferSend(int size)
            : this(new MemoryStream(size))
        {
        }

        public BoundedByteBufferSend(RequestOrResponse request) : this(request.SizeInBytes + ((request.RequestId.HasValue) ? 2 : 0))
        {
            if (request.RequestId.HasValue)
            {
                Buffer.PutShort(request.RequestId.Value);
            } 

            request.WriteTo(Buffer);
            Buffer.Position = 0;
        }

        public override int WriteTo(Stream channel)
        {
            this.ExpectIncomplete();
            var written = 0;
            channel.Write(this.sizeBuffer.GetBuffer(), 0, (int) sizeBuffer.Length);
            written += (int)sizeBuffer.Length;
            channel.Write(Buffer.GetBuffer(), 0, (int)Buffer.Length);
            written += (int)Buffer.Length;

            // custom: since .net Write doesn't return written bytes we assume that all was written.
            
            // if we are done, mark it off
            this.complete = true;
            return written;
        }
    }
}