namespace Kafka.Client.Messages
{
    using System.Collections.Generic;
    using System.IO;

    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Client.Utils;

    using Kafka.Client.Extensions;

    internal class ByteBufferMessageSetIterator : IteratorTemplate<MessageAndOffset>
    {
        private readonly ByteBufferMessageSet parent;

        private readonly ByteBuffer topIter;

        private readonly bool isShallow;

        private IIterator<MessageAndOffset> innerIter = null;

        public ByteBufferMessageSetIterator(ByteBufferMessageSet parent, bool isShallow)
        {
            this.parent = parent;
            this.topIter = parent.Buffer.Slice();
            this.isShallow = isShallow;
        }

        public bool InnerDone()
        {
            return this.innerIter == null || !innerIter.HasNext();
        }

        public MessageAndOffset MakeNextOuter()
        {
            // if there isn't at least an offset and size, we are done
            if (this.topIter.Remaining() < 12)
            {
                return this.AllDone();
            }
            var offset = topIter.GetLong();
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
            var message = topIter.Slice();
            message.Limit(size);
            topIter.Position = topIter.Position + size;
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
                        innerIter = ByteBufferMessageSet.Decompress(newMessage).InternalIterator();
                        if (!innerIter.HasNext())
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
                    return innerIter.Next();
                }
            }
        }
    }
}