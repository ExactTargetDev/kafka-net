namespace Kafka.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;

    using Kafka.Client.Messages;
    using Kafka.Client.Utils;

    using Xunit;

    using Util = Kafka.Tests.Utils.Util;

    public class MessageTest
    {

        [Fact]
        public void CreateMessage()
        {
            var msg = new Message(
                Encoding.UTF8.GetBytes("acd"), Encoding.UTF8.GetBytes("key1"), CompressionCodecs.NoCompressionCodec);


            Assert.True(msg.IsValid);
            Assert.Equal(CompressionCodecs.NoCompressionCodec, msg.CompressionCodec);
            Assert.Equal("key1", Util.ReadMemoryStream(msg.Key));
            Assert.Equal("abcd", Util.ReadMemoryStream(msg.Payload));


            var set = new ByteBufferMessageSet(CompressionCodecs.GZIPCompressionCodec, new List<Message> { msg });

            var iter = set.GetEnumerator();
            iter.MoveNext();
            var element = iter.Current;
            Console.WriteLine(element);
        } 
    }
}