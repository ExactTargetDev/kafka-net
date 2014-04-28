namespace Kafka.Client.Network
{
    using System.IO;

    using Kafka.Client.Common.Imported;
    using Kafka.Client.Extensions;

    /// <summary>
    /// Represents a communication between the client and server
    /// </summary>
    public class BoundedByteBufferReceive : Receive
    {
        private ByteBuffer sizeBuffer = ByteBuffer.Allocate(4);

        private ByteBuffer contentBuffer = null;

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
        public override ByteBuffer Buffer
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
            if (this.sizeBuffer.Remaining() > 0)
            {
                read += channel.Read(this.sizeBuffer.Array, 0, 4);
                this.sizeBuffer.Position = read;
            }

            // have we allocated the request buffer yet?
            if (this.contentBuffer == null && !this.sizeBuffer.HasRemaining())
            {
                this.sizeBuffer.Rewind();
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
                read = channel.Read(this.contentBuffer.Array, this.contentBuffer.ArrayOffset(), this.contentBuffer.Limit());
                this.contentBuffer.Position += read;

                // did we get everything?
                if (!this.contentBuffer.HasRemaining())
                {
                    this.contentBuffer.Rewind();
                    this.complete = true;
                }
            }
            return read;

        }

        private ByteBuffer ByteBufferAllocate(int size)
        {
            return ByteBuffer.Allocate(size);
        }
    }
}