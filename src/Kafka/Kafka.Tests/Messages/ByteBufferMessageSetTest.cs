namespace Kafka.Tests
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    using Kafka.Client.Messages;
    using Kafka.Tests.Utils;

    using Xunit;

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
            Assert.Equal(Util.EnumeratorToArray(this.messages.GetEnumerator()), Util.EnumeratorToArray(messageSet.Select(m => m.Message).GetEnumerator()));
        }

        private ByteBufferMessageSet CreateMessageSet(List<Message> list)
        {
            return new ByteBufferMessageSet(CompressionCodecs.NoCompressionCodec, list);
        }
    }
}