namespace Kafka.Tests.Messages
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    using Kafka.Client.Extensions;
    using Kafka.Client.Messages;

    using Xunit;

    public class MessageCompressionTest
    {
        [Fact]
        public void TestSimpleCompressDecompress()
        {
            var codecs = new List<CompressionCodecs> { CompressionCodecs.GZIPCompressionCodec, CompressionCodecs.SnappyCompressionCodec };

            foreach (var codec in codecs)
            {
                this.TestSimpleCompressDecompressInner(codec);
            }
        } 

        private void TestSimpleCompressDecompressInner(CompressionCodecs compressionCodec)
        {
            var messages = new List<Message>
                               {
                                   new Message(Encoding.UTF8.GetBytes("hi there")),
                                   new Message(Encoding.UTF8.GetBytes("I am fine")),
                                   new Message(Encoding.UTF8.GetBytes("I am not so well today")),
                               };
            var messageSet = new ByteBufferMessageSet(compressionCodec, messages);
            Assert.Equal(compressionCodec, messageSet.ShallowIterator().Next().Message.CompressionCodec);
            var decompressed = messageSet.Iterator().ToEnumerable().Select(x => x.Message).ToList();
            Assert.Equal(messages, decompressed);
        }

        [Fact]
        public void TestComplexCompressDecompress()
        {
            var messages = new List<Message>
                               {
                                   new Message(Encoding.UTF8.GetBytes("hi there")),
                                   new Message(Encoding.UTF8.GetBytes("I am fine")),
                                   new Message(Encoding.UTF8.GetBytes("I am not so well today")),
                               };
            var message = new ByteBufferMessageSet(CompressionCodecs.DefaultCompressionCodec, messages.Take(2).ToList());
            var complexMessages =
                new List<Message> { message.ShallowIterator().Next().Message }.Union(messages.Skip(2).Take(1).ToList())
                                                                              .ToList();
            var complexMessage = new ByteBufferMessageSet(CompressionCodecs.DefaultCompressionCodec, complexMessages);
            var decompressedMessages = complexMessage.Iterator().ToEnumerable().Select(x => x.Message).ToList();
            Assert.Equal(messages, decompressedMessages);
        }
    }
}