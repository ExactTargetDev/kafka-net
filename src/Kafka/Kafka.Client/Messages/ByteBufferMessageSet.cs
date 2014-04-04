using System.Collections.Generic;
using System.IO;

namespace Kafka.Client.Messages
{
    internal class ByteBufferMessageSet : MessageSet
    {

        private int shallowValidByteCount = -1;

        public MemoryStream Buffer { get; private set; }

        public ByteBufferMessageSet(MemoryStream buffer)
        {
            Buffer = buffer;
        }

        //TODO: impl me

        public int ValidBytes { get; set; }

        public IEnumerable<MessageAndOffset> ShallowEnumerator()
        {
            throw new System.NotImplementedException();
        }

        public override IEnumerator<MessageAndOffset> GetEnumerator()
        {
            throw new System.NotImplementedException();
        }

        public override int SizeInBytes
        {
            get { throw new System.NotImplementedException(); }
        }

        public override int WriteTo(Stream channel, long offset, int maxSize)
        {
            throw new System.NotImplementedException();
        }
    }
}