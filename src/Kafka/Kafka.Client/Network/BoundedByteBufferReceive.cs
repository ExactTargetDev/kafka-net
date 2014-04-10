namespace Kafka.Client.Network
{
    using System.IO;

    using Kafka.Client.Extensions;

    /// <summary>
    /// Represents a communication between the client and server
    /// </summary>
    public class BoundedByteBufferReceive : Receive
    {
        private MemoryStream sizeBuffer = new MemoryStream(new byte[4], 0, 4, true, true);

        private MemoryStream contentBuffer = null;

        public int MaxSize { get; private set; }

        public BoundedByteBufferReceive(int maxSize)
        {
            this.MaxSize = maxSize;
        }

        public BoundedByteBufferReceive()
            : this(int.MaxValue)
        {
        }

        /// <summary>
        /// Get the content buffer for this transmission
        /// </summary>
        public override MemoryStream Buffer
        {
            get
            {
                this.ExpectComplete();
                return this.contentBuffer;
            }
        }

        public override int ReadFrom(Stream channel)
        {
            this.ExpectIncomplete();
            var read = 0;

            // have we read the request size yet? 
            if (this.sizeBuffer.Position < 4)
            {
                read += channel.Read(this.sizeBuffer.GetBuffer(), 0, 4);
                this.sizeBuffer.Position = read;
            }

            // have we allocated the request buffer yet?
            if (this.contentBuffer == null && this.sizeBuffer.Position == 4)
            {
                this.sizeBuffer.Position = 0;
                var size = this.sizeBuffer.GetInt();
                if (size <= 0)
                {
                    throw new InvalidRequestException(string.Format("{0} is not a valid request size", size));
                }
                if (size > this.MaxSize)
                {
                    throw new InvalidRequestException(
                        string.Format(
                            "Request of length {0} is not valid, it is larget than the maximum size of {1} bytes",
                            size,
                            this.MaxSize));
                }
                this.contentBuffer = this.ByteBufferAllocate(size);
            }

            // if we have a buffer read some stuff into it
            if (this.contentBuffer != null)
            {
                read = channel.Read(this.contentBuffer.GetBuffer(), 0, (int)this.contentBuffer.Length);
                this.contentBuffer.Position += read;

                // did we get everything?
                if (this.contentBuffer.Position == this.contentBuffer.Length)
                {
                    this.contentBuffer.Position = 0;
                    this.complete = true;
                }
            }
            return read;

        }

        private MemoryStream ByteBufferAllocate(int size)
        {
            var stream = new MemoryStream(size);
            stream.SetLength(size);
            return stream;
        }
    }
}