namespace Kafka.Tests
{
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;

    using Kafka.Client.Messages;
    using Kafka.Tests.Utils;

    using Xunit;

    using Kafka.Client.Extensions;

    public class ByteBufferMessageSetTest
    {
        private List<Message> messages;

        public ByteBufferMessageSetTest()
        {
            this.messages = new List<Message>
                                {
                                    new Message(Encoding.UTF8.GetBytes("abcd")),
                                    new Message(Encoding.UTF8.GetBytes("efgh")),
                                    new Message(Encoding.UTF8.GetBytes("ijkl")),
                                };
        }

        [Fact]
        public void TestWrittenEqualsRead()
        {
            var messageSet = this.CreateMessageSet(this.messages);
            var msg0 = this.messages[0];
            Assert.Equal(0x12, msg0.Buffer.Length);
            var expected = Util.EnumeratorToArray(this.messages.GetEnumerator());
            var actual = Util.EnumeratorToArray(messageSet.Select(m => m.Message).GetEnumerator());
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void TestIteratorIsConsistent()
        {
            var m = this.CreateMessageSet(messages);
            var expected = Util.EnumeratorToArray(this.messages.GetEnumerator());
            var actual = Util.EnumeratorToArray(this.messages.GetEnumerator());
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void TestSizeInBytes()
        {
            Assert.Equal(0, this.CreateMessageSet(new List<Message>()).SizeInBytes);
            Assert.Equal(MessageSet.MessageSetSize(messages), this.CreateMessageSet(messages).SizeInBytes);
        }

        private ByteBufferMessageSet CreateMessageSet(List<Message> list)
        {
            return new ByteBufferMessageSet(CompressionCodecs.NoCompressionCodec, list);
        }
    }
}