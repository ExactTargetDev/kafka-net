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
        public void Crc()
        {

            var bytes = Encoding.UTF8.GetBytes("Ala ma kota i ten kota ma kota w glowie.");
           /* var bytes = new byte[] { 0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x04,
                0x6b,
                0x65,
                0x79,
                0x31,
                0x00,
                0x00,
                0x00,
                0x06,
                0x76,
                0x61,
                0x6c,
                0x75,
                0x65,
                0x31};*/

            var str = Encoding.UTF8.GetString(bytes, 4, 20);

            var crc = Crc32.Compute(bytes, 4, 20);

            var l  = Client.Utils.Util.Crc32(bytes, 4, 20);

            Console.WriteLine(l);
        }

        [Fact]
        public void CreateMessage()
        {
            var msg = new Message(
                Encoding.UTF8.GetBytes("value1"), Encoding.UTF8.GetBytes("key1"), CompressionCodecs.NoCompressionCodec);

            
           

            var crc = Crc32.Compute(new byte[] { 0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x04,
                0x6b,
                0x65,
                0x79,
                0x31,
                0x00,
                0x00,
                0x00,
                0x06,
                0x76,
                0x61,
                0x6c,
                0x75,
                0x65,
                0x31});

            Console.WriteLine(crc);

//            Assert.Equal(0x75F0F41D, msg.ComputeChecksum());

          //  var bytes = Encoding.UTF8.GetBytes("TO JEST JAKIS DLUGI TEKST");
          //  var crc = Crc32.Compute(bytes);


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