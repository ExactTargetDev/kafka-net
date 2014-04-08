namespace Kafka.Client.Network
{
    using System.IO;

    using Kafka.Client.Extensions;

    /// <summary>
    /// Represents a communication between the client and server
    /// </summary>
    public class BoundedByteBufferReceive : Receive
    {
        private MemoryStream sizeBuffer = new MemoryStream(4);

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
                this.sizeBuffer.SetLength(4); //TODO: rework it!
                read += channel.Read(this.sizeBuffer.GetBuffer(), 0, 4);
                this.sizeBuffer.Position = read;
            }

            // have we allocated the request buffer yet?
            if (this.contentBuffer == null && this.sizeBuffer.Position == 4)
            {
                this.sizeBuffer.Position = 0;
                var size = sizeBuffer.GetInt();
                if (size <= 0)
                {
                    throw new InvalidRequestException(string.Format("{0} is not a valid request size", size));
                }
                if (size > MaxSize)
                {
                    throw new InvalidRequestException(
                        string.Format(
                            "Request of length {0} is not valid, it is larget than the maximum size of {1} bytes",
                            size,
                            MaxSize));
                }
                contentBuffer = this.ByteBufferAllocate(size);
            }

            // if we have a buffer read some stuff into it
            if (contentBuffer != null)
            {
                read = channel.Read(contentBuffer.GetBuffer(), 0, (int)contentBuffer.Length);
                contentBuffer.Position += read;

                // did we get everything?
                if (contentBuffer.Position == contentBuffer.Length)
                {
                    contentBuffer.Position = 0;
                    complete = true;
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