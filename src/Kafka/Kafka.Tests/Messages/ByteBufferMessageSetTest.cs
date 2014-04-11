namespace Kafka.Tests
{
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;

    using Kafka.Client.Common.Imported;
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

        [Fact]
        public void TestWriteTo()
        {
            // test empty message set
            TestWriteToWithMessageSet(this.CreateMessageSet(new List<Message>()));
            TestWriteToWithMessageSet(this.CreateMessageSet(messages));
        }

        internal void TestWriteToWithMessageSet(MessageSet set)
        {
            // do the write twice to ensure the message set is restored to its orginal state
            foreach (var i in Enumerable.Range(0, 1))
            {
                var stream = new MemoryStream();
                var written = set.WriteTo(stream, 0, 1024);
                Assert.Equal(set.SizeInBytes, written);
                stream.Position = 0;
                var newSet = new ByteBufferMessageSet(stream);
                Assert.Equal(Util.EnumeratorToArray(set.GetEnumerator()), Util.EnumeratorToArray(newSet.GetEnumerator()));
            }
        }

        [Fact]
        public void TestValidBytes()
        {
            var messages = new ByteBufferMessageSet(
                CompressionCodecs.NoCompressionCodec,
                new List<Message>() 
                {
                  new Message(Encoding.UTF8.GetBytes("hello")),
                  new Message(Encoding.UTF8.GetBytes("there"))
                });
            var bufSize = messages.SizeInBytes + 2;
            var buffer = new MemoryStream(new byte[bufSize], 0, bufSize, true, true);
            buffer.Write(messages.Buffer.GetBuffer(), 0, (int)messages.Buffer.Length);
            buffer.PutShort(4);

            var messagesPlus = new ByteBufferMessageSet(buffer);
            Assert.Equal(messages.ValidBytes, messagesPlus.ValidBytes);

            // test valid bytes on empty ByteBufferMessageSet
            {
                Assert.Equal(0, MessageSet.Empty.ValidBytes);
            }

        }

        [Fact]
        public void TestValidBytesWithCompression()
        {
            var messages = new ByteBufferMessageSet(
                CompressionCodecs.DefaultCompressionCodec,
                new List<Message>() 
                {
                  new Message(Encoding.UTF8.GetBytes("hello")),
                  new Message(Encoding.UTF8.GetBytes("there"))
                });
            var bufSize = messages.SizeInBytes + 2;
            var buffer = new MemoryStream(new byte[bufSize], 0, bufSize, true, true);
            buffer.Write(messages.Buffer.GetBuffer(), 0, (int)messages.Buffer.Length);
            buffer.PutShort(4);

            var messagesPlus = new ByteBufferMessageSet(buffer);
            Assert.Equal(messages.ValidBytes, messagesPlus.ValidBytes);
        }

        [Fact]
        public void TestEquals()
        {
            var messages = new ByteBufferMessageSet(
                CompressionCodecs.DefaultCompressionCodec,
                new List<Message>() 
                {
                  new Message(Encoding.UTF8.GetBytes("hello")),
                  new Message(Encoding.UTF8.GetBytes("there"))
                });
            var moreMessages = new ByteBufferMessageSet(
                CompressionCodecs.DefaultCompressionCodec,
                new List<Message>() 
                {
                  new Message(Encoding.UTF8.GetBytes("hello")),
                  new Message(Encoding.UTF8.GetBytes("there"))
                });

            Assert.True(messages.Equals(moreMessages));

            messages = new ByteBufferMessageSet(
               CompressionCodecs.NoCompressionCodec,
               new List<Message>() 
                {
                  new Message(Encoding.UTF8.GetBytes("hello")),
                  new Message(Encoding.UTF8.GetBytes("there"))
                });
            moreMessages = new ByteBufferMessageSet(
                CompressionCodecs.NoCompressionCodec,
                new List<Message>() 
                {
                  new Message(Encoding.UTF8.GetBytes("hello")),
                  new Message(Encoding.UTF8.GetBytes("there"))
                });

            Assert.True(messages.Equals(moreMessages));
        }

        [Fact]
        public void TestIterator()
        {
            var messageList = new List<Message>
                                  {
                                      new Message(Encoding.UTF8.GetBytes("msg1")),
                                      new Message(Encoding.UTF8.GetBytes("msg2")),
                                      new Message(Encoding.UTF8.GetBytes("msg3")),
                                  };

            // test for uncompressed regular messages
            {
                var messageSet = new ByteBufferMessageSet(CompressionCodecs.NoCompressionCodec, messageList);
                TestUtils.CheckEquals<Message>(
                    messageList.GetEnumerator(), TestUtils.GetMessageIterator(messageSet.GetEnumerator()));
                // make sure ByteBufferMessageSet is re-iterable.
                TestUtils.CheckEquals<Message>(
                    messageList.GetEnumerator(), TestUtils.GetMessageIterator(messageSet.GetEnumerator()));

                // make sure shallow iterator is the same as deep iterator
                TestUtils.CheckEquals<Message>(
                    TestUtils.GetMessageIterator(messageSet.ShallowEnumerator()),
                    TestUtils.GetMessageIterator(messageSet.GetEnumerator()));
            }

            // test for compressed regular messages
            {
                 var messageSet = new ByteBufferMessageSet(CompressionCodecs.DefaultCompressionCodec, messageList);
                TestUtils.CheckEquals<Message>(
                    messageList.GetEnumerator(), TestUtils.GetMessageIterator(messageSet.GetEnumerator()));
                // make sure ByteBufferMessageSet is re-iterable.
                TestUtils.CheckEquals<Message>(
                    messageList.GetEnumerator(), TestUtils.GetMessageIterator(messageSet.GetEnumerator()));

            }

            // test for mixed empty and non-empty messagesets uncompressed
            {
                List<Message> emptyMessageList = null;
                var emptyMessageSet = new ByteBufferMessageSet(CompressionCodecs.NoCompressionCodec, emptyMessageList);
                var regularMessageSet = new ByteBufferMessageSet(CompressionCodecs.NoCompressionCodec, messageList);
                var bufferLength = emptyMessageSet.Buffer.Length + regularMessageSet.Buffer.Length;
                var buffer = new MemoryStream(new byte[bufferLength], 0, (int)bufferLength, true, true);
                buffer.Write(emptyMessageSet.Buffer.GetBuffer(), 0, (int)emptyMessageSet.Buffer.Length);
                buffer.Write(regularMessageSet.Buffer.GetBuffer(), 0, (int)regularMessageSet.Buffer.Length);
                buffer.Position = 0;
                var mixedMessageSet = new ByteBufferMessageSet(buffer);
                TestUtils.CheckEquals<Message>(
                    messageList.GetEnumerator(), TestUtils.GetMessageIterator(mixedMessageSet.GetEnumerator()));

                // make sure ByteBufferMessageSet is re-iterable.
                TestUtils.CheckEquals<Message>(
                    messageList.GetEnumerator(), TestUtils.GetMessageIterator(mixedMessageSet.GetEnumerator()));

                // make sure shallow iterator is the same as deep iterator
                TestUtils.CheckEquals<Message>(
                    TestUtils.GetMessageIterator(mixedMessageSet.ShallowEnumerator()),
                    TestUtils.GetMessageIterator(mixedMessageSet.GetEnumerator()));
            }

             // test for mixed empty and non-empty messagesets compressed
            {
                List<Message> emptyMessageList = null;
                var emptyMessageSet = new ByteBufferMessageSet(CompressionCodecs.DefaultCompressionCodec, emptyMessageList);
                var regularMessageSet = new ByteBufferMessageSet(CompressionCodecs.DefaultCompressionCodec, messageList);
                var bufferLength = emptyMessageSet.Buffer.Length + regularMessageSet.Buffer.Length;
                var buffer = new MemoryStream(new byte[bufferLength], 0, (int)bufferLength, true, true);
                buffer.Write(emptyMessageSet.Buffer.GetBuffer(), 0, (int)emptyMessageSet.Buffer.Length);
                buffer.Write(regularMessageSet.Buffer.GetBuffer(), 0, (int)regularMessageSet.Buffer.Length);
                buffer.Position = 0;
                var mixedMessageSet = new ByteBufferMessageSet(buffer);
                TestUtils.CheckEquals<Message>(
                    messageList.GetEnumerator(), TestUtils.GetMessageIterator(mixedMessageSet.GetEnumerator()));

                // make sure ByteBufferMessageSet is re-iterable.
                TestUtils.CheckEquals<Message>(
                    messageList.GetEnumerator(), TestUtils.GetMessageIterator(mixedMessageSet.GetEnumerator()));

                this.VerifyShallowIterator(mixedMessageSet);
            }
        }

        [Fact]
        public void TestOffsetAssigment()
        {
            var messages = new ByteBufferMessageSet(CompressionCodecs.NoCompressionCodec, new List<Message>
                                                                                              {
                                                                                                  new Message(Encoding.UTF8.GetBytes("hello")),
                                                                                                  new Message(Encoding.UTF8.GetBytes("there")),
                                                                                                  new Message(Encoding.UTF8.GetBytes("beautiful")),
                                                                                              });

            var compressedMessages = new ByteBufferMessageSet(
                CompressionCodecs.DefaultCompressionCodec, messages.Select(m => m.Message).ToList());

            // check uncompressed offsets 
            CheckOffsets(messages, 0);
            var offset = 1234567;
            CheckOffsets(messages.AssignOffsets(new AtomicLong(offset), CompressionCodecs.NoCompressionCodec), offset);

            // check compressed offset
            CheckOffsets(compressedMessages, 0);
            CheckOffsets(
                compressedMessages.AssignOffsets(new AtomicLong(offset), CompressionCodecs.DefaultCompressionCodec),
                offset);
        }

        internal void CheckOffsets(ByteBufferMessageSet messages, long baseOffset)
        {
            var offset = baseOffset;
            foreach (var entry in messages)
            {
                Assert.Equal(offset, entry.Offset);
                offset++;
            }
        }

        internal void VerifyShallowIterator(ByteBufferMessageSet messageSet)
        {
            // make sure the offsets returned by a shallow iterator is a subset of that of a deep iterator
            var shallowOffsets =
                new HashSet<long>(
                    Util.EnumeratorToArray(messageSet.ShallowEnumerator()).Select(msgAndOff => msgAndOff.Offset));
            var deepOffsets = new HashSet<long>(
                Util.EnumeratorToArray(messageSet.GetEnumerator()).Select(msgAndOff => msgAndOff.Offset)
                );
            Assert.True(shallowOffsets.IsSubsetOf(deepOffsets));
        }

        private ByteBufferMessageSet CreateMessageSet(List<Message> list)
        {
            return new ByteBufferMessageSet(CompressionCodecs.NoCompressionCodec, list);
        }


    }
}