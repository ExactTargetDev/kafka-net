namespace Kafka.Client.Messages
{
    using System.Collections.Generic;
    using System.IO;

    using Kafka.Client.Common;
    using Kafka.Client.Utils;

    using Kafka.Client.Extensions;

    internal class ByteBufferMessageSetEnumerator : IteratorTemplate<MessageAndOffset>
    {
        private readonly ByteBufferMessageSet parent;

        private readonly MemoryStream topIter;

        private readonly bool isShallow;

        private IEnumerator<MessageAndOffset> innerIter = null;

        public ByteBufferMessageSetEnumerator(ByteBufferMessageSet parent, bool isShallow)
        {
            this.parent = parent;
            this.topIter = new MemoryStream(parent.Buffer.GetBuffer(), (int)parent.Buffer.Position, (int)(parent.Buffer.Length - parent.Buffer.Position));
            this.isShallow = isShallow;
        }

        public bool InnerDone()
        {
            return this.innerIter == null || !this.innerIter.MoveNext();
        }

        public MessageAndOffset MakeNextOuter()
        {
            // if there isn't at least an offset and size, we are done
            if (this.topIter.Length - this.topIter.Position < 12)
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
            if (this.topIter.Length - this.topIter.Position < size)
            {
                return this.AllDone();
            }

            // read the current message and check correctness
            byte[] messagePayload = new byte[size];
            topIter.Read(messagePayload, 0, messagePayload.Length);
            var newMessage = new Message(new MemoryStream(messagePayload, 0, messagePayload.Length, true, true));

            if (isShallow)
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
                        if (!this.innerIter.MoveNext())
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
                    this.innerIter.MoveNext();
                    return this.innerIter.Current;
                }
            }
        }

        public override MessageAndOffset Current
        {
            get
            {
                return base.Current;
            }
        }
    }
}