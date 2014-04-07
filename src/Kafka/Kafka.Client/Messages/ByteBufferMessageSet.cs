using System.Collections.Generic;
using System.IO;

namespace Kafka.Client.Messages
{
    using System.Collections;

    using Kafka.Client.Common.Imported;

    using System.Linq;

    using Kafka.Client.Serializers;

    using Kafka.Client.Extensions;

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
            Buffer = buffer;
        }

        public ByteBufferMessageSet(CompressionCodecs compressionCodec, IEnumerable<Message> messages) : this(ByteBufferMessageSet.Create(new AtomicLong(0), compressionCodec, messages))
        {
        }

        public ByteBufferMessageSet(
            CompressionCodecs compressionCodec, AtomicLong offsetCounter, IEnumerable<Message> messages)
            : this(ByteBufferMessageSet.Create(offsetCounter, compressionCodec, messages))
        {
        }

        public ByteBufferMessageSet(IEnumerable<Message> messages)
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
            return shallowValidByteCount;
        }

        public override int WriteTo(Stream channel, long offset, int size)
        {
            //TODO: buffer.mark and reset?
            var written = 0;
            while (written < SizeInBytes)
            {
                channel.Write(Buffer.GetBuffer(), 0, (int) this.Buffer.Length);
                written += (int) this.Buffer.Length;
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


        //TODO: private[kafka] def assignOffsets(offsetCounter: AtomicLong, codec: CompressionCodec): ByteBufferMessageSet = {

        public override int SizeInBytes
        {
            get
            {
                return (int)Buffer.Length; 
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
            AtomicLong offsetCounter, CompressionCodecs compressionCodec, IEnumerable<Message> messages)
        {
            if (messages.Count() == 0)
            {
                return Empty.Buffer;
            } else if (CompressionCodecs.NoCompressionCodec == compressionCodec)
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
                byte[] bytes = new byte[MessageSetSize(messages)];
                var byteArrayStream = new MemoryStream(bytes);
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

                var msg = new Message(bytes, compressionCodec);
                var result = new MemoryStream(msg.Size + MessageSet.LogOverhead);
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
                int read = 0;
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
            buffer.Write(message.Buffer.GetBuffer(), 0, (int) message.Buffer.Length);
            message.Buffer.Position = 0;
        }
    }
}