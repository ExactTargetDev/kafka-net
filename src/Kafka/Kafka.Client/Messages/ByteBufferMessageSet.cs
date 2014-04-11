namespace Kafka.Client.Messages
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    using Kafka.Client.Common.Imported;
    using Kafka.Client.Extensions;
    using Kafka.Client.Serializers;

    /// <summary>
    /// A sequence of messages stored in a byte buffer
    /// 
    /// There are two ways to create a ByteBufferMessageSet
    /// 
    /// Option 1: From a ByteBuffer which already contains the serialized message set. Consumers will use this method.
    /// 
    /// Option 2: Give it a list of messages along with instructions relating to serialization format. Producers will use this method.
    /// </summary>
    internal class ByteBufferMessageSet : MessageSet
    {

        private int shallowValidByteCount = -1;

        public MemoryStream Buffer { get; private set; }

        public ByteBufferMessageSet(MemoryStream buffer)
        {
            this.Buffer = buffer;
        }

        public ByteBufferMessageSet(CompressionCodecs compressionCodec, List<Message> messages)
            : this(Create(new AtomicLong(0), compressionCodec, messages))
        {
        }

        public ByteBufferMessageSet(
            CompressionCodecs compressionCodec, AtomicLong offsetCounter, List<Message> messages)
            : this(Create(offsetCounter, compressionCodec, messages))
        {
        }

        public ByteBufferMessageSet(List<Message> messages)
            : this(CompressionCodecs.NoCompressionCodec, new AtomicLong(0), messages)
        {
        }

        private int ShallowValidBytes()
        {
            if (this.shallowValidByteCount < 0)
            {
                var bytes = 0;
                var iter = this.InternalIterator(true);
                while (iter.MoveNext())
                {
                    var messageAndOffset = iter.Current;
                    bytes += MessageSet.EntrySize(messageAndOffset.Message);
                }

                this.shallowValidByteCount = bytes;
            }
            return this.shallowValidByteCount;
        }

        public override int WriteTo(Stream channel, long offset, int size)
        {
            // Ignore offset and size from input. We just want to write the whole buffer to the channel.
            var written = 0;
            while (written < this.SizeInBytes)
            {
                channel.Write(this.Buffer.GetBuffer(), 0, (int)this.Buffer.Length);
                written += (int)this.Buffer.Length;
            }

            return written;
        }


        /// <summary>
        /// default iterator that iterates over decompressed messages
        /// </summary>
        /// <returns></returns>
        public override IEnumerator<MessageAndOffset> GetEnumerator()
        {
            return this.InternalIterator();
        }

        public IEnumerator<MessageAndOffset> ShallowEnumerator()
        {
            return this.InternalIterator(true);
        }

        public IEnumerator<MessageAndOffset> InternalIterator(bool isShallow = false)
        {
            return new ByteBufferMessageSetEnumerator(this, isShallow);
        }

        internal ByteBufferMessageSet AssignOffsets(AtomicLong offsetCounter, CompressionCodecs codec)
        {
            if (codec == CompressionCodecs.NoCompressionCodec)
            {
                // do as in-place conversion
                var position = 0;
                var markedPosition = this.Buffer.Position;
                while (position < this.SizeInBytes - MessageSet.LogOverhead)
                {
                    this.Buffer.Position = position;
                    this.Buffer.PutLong(offsetCounter.GetAndIncrement());
                    position += MessageSet.LogOverhead + Buffer.GetInt();
                }
                this.Buffer.Position = markedPosition;
                return this;
            }
            else
            {
                // messages are compressed, crack open the messageset and recompress with correct offset
                var messages = this.InternalIterator(isShallow: false).ToEnumerable().Select(_ => _.Message);
                return new ByteBufferMessageSet(codec, offsetCounter, messages.ToList());
            }
        }

        public override int SizeInBytes
        {
            get
            {
                return (int)this.Buffer.Length; 
            }
        }

        public int ValidBytes
        {
            get
            {
                return this.ShallowValidBytes();
            }
        }

        private static MemoryStream Create(
            AtomicLong offsetCounter, CompressionCodecs compressionCodec, List<Message> messages)
        {
            if (messages == null || !messages.Any())
            {
                return Empty.Buffer;
            } 
            else if (CompressionCodecs.NoCompressionCodec == compressionCodec)
            {
                var buffer = new MemoryStream(MessageSetSize(messages));
                foreach (var message in messages)
                {
                    WriteMessage(buffer, message, offsetCounter.GetAndIncrement());
                }
                buffer.Position = 0;
                return buffer;
            }
            else
            {
                var byteArrayStream = new MemoryStream(MessageSetSize(messages));
                var offset = -1L;

                using (var output = new KafkaBinaryWriter(CompressionFactory.BuildWriter(compressionCodec, byteArrayStream)))
                {
                    
                    foreach (var message in messages)
                    {
                        offset = offsetCounter.GetAndIncrement();
                        output.Write(offset);
                        output.Write(message.Size);
                        output.Write(message.Buffer.GetBuffer(), 0, (int) message.Buffer.Length);
                    }
                }

                var msg = new Message(byteArrayStream.ToArray(), compressionCodec);
                var result = new MemoryStream(msg.Size + LogOverhead);
                WriteMessage(result, msg, offset);
                result.Position = 0;
                return result;
            }
        }


        public static ByteBufferMessageSet Decompress(Message message)
        {
            var outputStream = new MemoryStream();
            var inputStream = message.Payload;
            var intermediateBuffer = new byte[1024];

            using (var compressed = CompressionFactory.BuildReader(message.CompressionCodec, inputStream))
            {
                var read = 0;
                while ((read = compressed.Read(intermediateBuffer, 0, 1024)) > 0)
                {
                    outputStream.Write(intermediateBuffer, 0, read);
                }
            }

            var outputBuffer = new MemoryStream((int)outputStream.Length);
            outputBuffer.Write(outputStream.GetBuffer(), 0, (int)outputStream.Length);
            outputBuffer.Position = 0;
            return new ByteBufferMessageSet(outputBuffer);
        }

        private static void WriteMessage(MemoryStream buffer, Message message, long offset)
        {
            buffer.PutLong(offset);
            buffer.PutInt(message.Size);
            buffer.Write(message.Buffer.GetBuffer(), 0, (int)message.Buffer.Length);
            message.Buffer.Position = 0;
        }

        protected bool Equals(ByteBufferMessageSet other)
        {
            if (this.Buffer.Length != other.Buffer.Length)
            {
                return false;
            }
            var pos1 = this.Buffer.Position;
            var pos2 = other.Buffer.Position;
            try
            {
                for (var i = 0; i < this.Buffer.Length; i++)
                {
                    if (this.Buffer.ReadByte() != other.Buffer.ReadByte())
                    {
                        return false;
                    }
                }
            }
            finally
            {
                this.Buffer.Position = pos1;
                other.Buffer.Position = pos2;
            }

            return true;

        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }
            if (ReferenceEquals(this, obj))
            {
                return true;
            }
            if (obj.GetType() != this.GetType())
            {
                return false;
            }
            return Equals((ByteBufferMessageSet)obj);
        }

        public override int GetHashCode()
        {
            return (this.Buffer != null ? this.Buffer.GetHashCode() : 0);
        }
    }
}