namespace Kafka.Client.Messages
{
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    using Kafka.Client.Common.Imported;
    using Kafka.Client.Extensions;
    using Kafka.Client.Serializers;
    using Kafka.Client.Utils;

    /// <summary>
    /// A sequence of messages stored in a byte buffer
    /// 
    /// There are two ways to create a ByteBufferMessageSet
    /// 
    /// Option 1: From a ByteBuffer which already contains the serialized message set. Consumers will use this method.
    /// 
    /// Option 2: Give it a list of messages along with instructions relating to serialization format. Producers will use this method.
    /// </summary>
    public class ByteBufferMessageSet : MessageSet
    {
        private static ByteBuffer Create(
           AtomicLong offsetCounter, CompressionCodecs compressionCodec, List<Message> messages)
        {
            if (messages == null || !messages.Any())
            {
                return Empty.Buffer;
            }
            else if (CompressionCodecs.NoCompressionCodec == compressionCodec)
            {
                var buffer = ByteBuffer.Allocate(MessageSetSize(messages));
                foreach (var message in messages)
                {
                    WriteMessage(buffer, message, offsetCounter.GetAndIncrement());
                }

                buffer.Rewind();
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
                        output.Write(message.Buffer.Array, message.Buffer.ArrayOffset(), message.Buffer.Limit());
                    }
                }

                var bytes = byteArrayStream.ToArray();
                var msg = new Message(bytes, compressionCodec);
                var buffer = ByteBuffer.Allocate(msg.Size + LogOverhead);
                WriteMessage(buffer, msg, offset);
                buffer.Rewind();
                return buffer;
            }
        }

        public static ByteBufferMessageSet Decompress(Message message)
        {
            var outputStream = new MemoryStream();
            var inputStream = message.Payload;
            var intermediateBuffer = new byte[1024];

            using (var compressed = CompressionFactory.BuildReader(message.CompressionCodec, inputStream))
            {
                int read;
                while ((read = compressed.Read(intermediateBuffer, 0, 1024)) > 0)
                {
                    outputStream.Write(intermediateBuffer, 0, read);
                }
            }

            var outputBuffer = ByteBuffer.Allocate((int)outputStream.Length);
            outputBuffer.Put(outputStream.ToArray());
            outputBuffer.Rewind();
            return new ByteBufferMessageSet(outputBuffer);
        }

        private static void WriteMessage(ByteBuffer buffer, Message message, long offset)
        {
            buffer.PutLong(offset);
            buffer.PutInt(message.Size);
            buffer.Put(message.Buffer);
            message.Buffer.Position = 0;
        }

        private int shallowValidByteCount = -1;

        public ByteBuffer Buffer { get; private set; }

        public ByteBufferMessageSet(ByteBuffer buffer)
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
                while (iter.HasNext())
                {
                    var messageAndOffset = iter.Next();
                    bytes += EntrySize(messageAndOffset.Message);
                }

                this.shallowValidByteCount = bytes;
            }

            return this.shallowValidByteCount;
        }

        public override int WriteTo(Stream channel, long offset, int size)
        {
            // Ignore offset and size from input. We just want to write the whole buffer to the channel.
            this.Buffer.Mark();
            var written = 0;
            while (written < this.SizeInBytes)
            {
                channel.Write(this.Buffer.Array, this.Buffer.ArrayOffset(), this.Buffer.Limit());
                written += (int)this.Buffer.Length;
            }

            this.Buffer.Reset();
            return written;
        }

        public override IIterator<MessageAndOffset> Iterator()
        {
            return this.InternalIterator();
        }

        public IIterator<MessageAndOffset> ShallowIterator()
        {
            return this.InternalIterator(true);
        }

        public IIterator<MessageAndOffset> InternalIterator(bool isShallow = false)
        {
            return new ByteBufferMessageSetIterator(this, isShallow);
        }

        internal ByteBufferMessageSet AssignOffsets(AtomicLong offsetCounter, CompressionCodecs codec)
        {
            if (codec == CompressionCodecs.NoCompressionCodec)
            {
                // do as in-place conversion
                var position = 0;
                this.Buffer.Mark();
                while (position < this.SizeInBytes - MessageSet.LogOverhead)
                {
                    this.Buffer.Position = position;
                    this.Buffer.PutLong(offsetCounter.GetAndIncrement());
                    position += MessageSet.LogOverhead + this.Buffer.GetInt();
                }

                this.Buffer.Reset();
                return this;
            }
            else
            {
                // messages are compressed, crack open the messageset and recompress with correct offset
                var messages = this.InternalIterator(isShallow:false).ToEnumerable().Select(_ => _.Message);
                return new ByteBufferMessageSet(codec, offsetCounter, messages.ToList());
            }
        }

        public override int SizeInBytes
        {
            get
            {
                return this.Buffer.Limit();
            }
        }

        public int ValidBytes
        {
            get
            {
                return this.ShallowValidBytes();
            }
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

            return this.Equals((ByteBufferMessageSet)obj);
        }

        public override int GetHashCode()
        {
            return this.Buffer != null ? this.Buffer.GetHashCode() : 0;
        }
    }

    internal class ByteBufferMessageSetIterator : IteratorTemplate<MessageAndOffset>
    {
        private readonly ByteBufferMessageSet parent;

        private readonly ByteBuffer topIter;

        private readonly bool isShallow;

        private IIterator<MessageAndOffset> innerIter;

        public ByteBufferMessageSetIterator(ByteBufferMessageSet parent, bool isShallow)
        {
            this.parent = parent;
            this.topIter = parent.Buffer.Slice();
            this.isShallow = isShallow;
        }

        public bool InnerDone()
        {
            return this.innerIter == null || !this.innerIter.HasNext();
        }

        public MessageAndOffset MakeNextOuter()
        {
            // if there isn't at least an offset and size, we are done
            if (this.topIter.Remaining() < 12)
            {
                return this.AllDone();
            }

            var offset = this.topIter.GetLong();
            var size = this.topIter.GetInt();
            if (size < Message.MinHeaderSize)
            {
                throw new InvalidMessageException("Message found with corrupt size (" + size + ")");
            }

            // we have an incomplete message
            if (this.topIter.Remaining() < size)
            {
                return this.AllDone();
            }

            // read the current message and check correctness
            var message = this.topIter.Slice();
            message.Limit(size);
            this.topIter.Position = this.topIter.Position + size;
            var newMessage = new Message(message);

            if (this.isShallow)
            {
                return new MessageAndOffset(newMessage, offset);
            }
            else
            {
                switch (newMessage.CompressionCodec)
                {
                    case CompressionCodecs.NoCompressionCodec:
                        this.innerIter = null;
                        return new MessageAndOffset(newMessage, offset);
                    default:
                        this.innerIter = ByteBufferMessageSet.Decompress(newMessage).InternalIterator();
                        if (!this.innerIter.HasNext())
                        {
                            this.innerIter = null;
                        }

                        return this.MakeNext();
                }
            }
        }

        protected override MessageAndOffset MakeNext()
        {
            if (this.isShallow)
            {
                return this.MakeNextOuter();
            }
            else
            {
                if (this.InnerDone())
                {
                    return this.MakeNextOuter();
                }
                else
                {
                    return this.innerIter.Next();
                }
            }
        }
    }
}