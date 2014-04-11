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

        private bool? innerHasNext = null;

        public ByteBufferMessageSetEnumerator(ByteBufferMessageSet parent, bool isShallow)
        {
            this.parent = parent;
            this.topIter = new MemoryStream(parent.Buffer.GetBuffer(), (int)parent.Buffer.Position, (int)(parent.Buffer.Length - parent.Buffer.Position), false, false);
            this.isShallow = isShallow;
        }

        private bool InnerHasNext()
        {
            if (this.innerHasNext.HasValue)
            {
                return this.innerHasNext.Value;
            }
            this.innerHasNext = this.innerIter.MoveNext();
            return this.innerHasNext.Value;
        }

        public bool InnerDone()
        {
            return this.innerIter == null || !this.InnerHasNext();
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
            var messagePayload = new byte[size];
            this.topIter.Read(messagePayload, 0, messagePayload.Length);
            var newMessage = new Message(new MemoryStream(messagePayload, 0, messagePayload.Length, true, true));

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
                        if (!this.InnerHasNext())
                        {
                            this.innerIter = null;
                            this.innerHasNext = null;
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
                    var current = this.innerIter.Current;
                    innerHasNext = null;
                    return current;
                }
            }
        }
    }
}