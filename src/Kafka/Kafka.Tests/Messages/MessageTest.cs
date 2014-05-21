namespace Kafka.Tests.Messages
{
    using System.Collections.Generic;
    using System.Text;

    using Kafka.Client.Common.Imported;
    using Kafka.Client.Messages;
    using Kafka.Tests.Utils;

    using Xunit;

    public class MessageTest
    {
         private List<MessageTestVal> messages = new List<MessageTestVal>();

        public MessageTest()
        {
            var keys = new List<byte[]> { null, Encoding.UTF8.GetBytes("key"), Encoding.UTF8.GetBytes(string.Empty) };
            var vals = new List<byte[]> { Encoding.UTF8.GetBytes("value"), Encoding.UTF8.GetBytes(string.Empty), null };
            var codecs = new List<CompressionCodecs>
                             {
                                 CompressionCodecs.NoCompressionCodec,
                                 CompressionCodecs.GZIPCompressionCodec
                             };
            foreach (var k in keys)
            {
                foreach (var v in vals)
                {
                    foreach (var codec in codecs)
                    {
                        this.messages.Add(new MessageTestVal(k, v, codec, new Message(v, k, codec)));
                    }
                }
            }
        }

        [Fact]
        public void TestFieldValues()
        {
            foreach (var v in this.messages)
            {
                if (v.Payload == null)
                {
                    Assert.True(v.Message.IsNull());
                    Assert.Equal(null, v.Message.Payload);
                }
                else
                {
                    TestUtils.CheckEquals(ByteBuffer.Wrap(v.Payload), v.Message.Payload);
                }
                Assert.Equal(Message.CurrentMagicValue, v.Message.Magic);
                if (v.Message.HasKey)
                {
                    TestUtils.CheckEquals(ByteBuffer.Wrap(v.Key), v.Message.Key);
                }
                else
                {
                    Assert.Equal(null, v.Message.Key);
                }
                Assert.Equal(v.Codec, v.Message.CompressionCodec);
            }
        }

        [Fact]
        public void TestChecksum()
        {
            foreach (var v in this.messages)
            {
                Assert.False(v.Message.Equals(null));
                Assert.False(v.Message.Equals("asdf"));
                Assert.True(v.Message.Equals(v.Message));
                var copy = new Message(v.Payload, v.Key, v.Codec);
                Assert.True(v.Message.Equals(copy));
            }
        }

        [Fact]
        public void TestIsHashable()
        {
            // this is silly, but why not
            var m = new Dictionary<Message, Message>();
            foreach (var v in this.messages)
            {
                m[v.Message] = v.Message;
            }

            foreach (var v in this.messages)
            {
                Assert.Equal(v.Message, m[v.Message]);   
            }
        }
    }

    internal class MessageTestVal
    {
        public byte[] Key { get; private set; }

        public byte[] Payload { get; private set; }

        public CompressionCodecs Codec { get; private set; }

        public Message Message { get; private set; }

        public MessageTestVal(byte[] key, byte[] payload, CompressionCodecs codec, Message message)
        {
            this.Key = key;
            this.Payload = payload;
            this.Codec = codec;
            this.Message = message;
        }
    }
}