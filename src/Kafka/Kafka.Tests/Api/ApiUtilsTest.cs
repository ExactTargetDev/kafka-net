namespace Kafka.Tests.Api
{
    using System;

    using Kafka.Client.Api;
    using Kafka.Client.Common;
    using Kafka.Client.Common.Imported;
    using Kafka.Tests.Custom.Extensions;
    using Kafka.Tests.Utils;

    using Xunit;

    public class ApiUtilsTest
    {
        private static Random rnd = new Random();

        [Fact]
         public void TestShortStringNonASCII()
         {
             // Random-length strings
            for (var i = 0; i < 100; i++)
            {
                // Since we're using UTF-8 encoding, each encoded byte will be one to four bytes long 
                var s = rnd.NextString(Math.Abs(rnd.Next() % (short.MaxValue / 4)));
                var bb = ByteBuffer.Allocate(ApiUtils.ShortStringLength(s));
                ApiUtils.WriteShortString(bb, s);
                bb.Rewind();
                Assert.Equal(s, ApiUtils.ReadShortString(bb));
            }
         }

        [Fact]
        public void TestShortStringASCII()
        {
             // Random-length strings
            for (var i = 0; i < 100; i++)
            {
                var s = TestUtils.RandomString(Math.Abs(rnd.Next() % short.MaxValue));
                var bb = ByteBuffer.Allocate(ApiUtils.ShortStringLength(s));
                ApiUtils.WriteShortString(bb, s);
                bb.Rewind();
                Assert.Equal(s, ApiUtils.ReadShortString(bb));
            }

            // Max size string
            var s1 = TestUtils.RandomString(short.MaxValue);
            var bb1 = ByteBuffer.Allocate(ApiUtils.ShortStringLength(s1));
            ApiUtils.WriteShortString(bb1, s1);
            bb1.Rewind();
            Assert.Equal(s1, ApiUtils.ReadShortString(bb1));

            // One byte too big
            var s2 = TestUtils.RandomString(short.MaxValue + 1);
            Assert.Throws<KafkaException>(() => ApiUtils.ShortStringLength(s2));
            Assert.Throws<KafkaException>(() => ApiUtils.WriteShortString(bb1, s2));
        }
    }
}