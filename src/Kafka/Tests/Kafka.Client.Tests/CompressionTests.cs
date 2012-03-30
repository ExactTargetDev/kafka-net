/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Kafka.Client.Tests
{
    using System.Collections.Generic;
    using System.Text;
    using Kafka.Client.Messages;
    using NUnit.Framework;

    [TestFixture]
    public class CompressionTests
    {
        [Test]
        public void CompressAndDecompressMessageUsingDefaultCompressionCodec()
        {
            byte[] messageBytes = new byte[] { 1, 2, 3, 4, 5 };
            Message message = new Message(messageBytes, CompressionCodecs.DefaultCompressionCodec);
            Message compressedMsg = CompressionUtils.Compress(new List<Message>() { message });
            var decompressed = CompressionUtils.Decompress(compressedMsg);
            int i = 0;
            foreach (var decompressedMessage in decompressed.Messages)
            {
                i++;
                Assert.AreEqual(message.Payload, decompressedMessage.Payload);
            }

            Assert.AreEqual(1, i);
        }

        [Test]
        public void CompressAndDecompress3MessagesUsingDefaultCompressionCodec()
        {
            byte[] messageBytes = new byte[] { 1, 2, 3, 4, 5 };
            Message message1 = new Message(messageBytes, CompressionCodecs.DefaultCompressionCodec);
            Message message2 = new Message(messageBytes, CompressionCodecs.DefaultCompressionCodec);
            Message message3 = new Message(messageBytes, CompressionCodecs.DefaultCompressionCodec);
            Message compressedMsg = CompressionUtils.Compress(new List<Message>() { message1, message2, message3 });
            var decompressed = CompressionUtils.Decompress(compressedMsg);
            int i = 0;
            foreach (var decompressedMessage in decompressed.Messages)
            {
                i++;
                Assert.AreEqual(message1.Payload, decompressedMessage.Payload);
            }

            Assert.AreEqual(3, i);
        }

        [Test]
        public void CreateCompressedBufferedMessageSet()
        {
            string testMessage = "TestMessage";
            Message message = new Message(Encoding.UTF8.GetBytes(testMessage));
            BufferedMessageSet bms = new BufferedMessageSet(CompressionCodecs.DefaultCompressionCodec, new List<Message>() { message });
            foreach (var bmsMessage in bms.Messages)
            {
                Assert.AreNotEqual(bmsMessage.Payload, message.Payload);
                var decompressedBms = CompressionUtils.Decompress(bmsMessage);
                foreach (var decompressedMessage in decompressedBms.Messages)
                {
                    Assert.AreEqual(message.ToString(), decompressedMessage.ToString());
                }
            }
        }

        [Test]
        public void CompressionUtilsTryToCompressWithNoCompresssionCodec()
        {
            string payload1 = "kafka 1.";
            byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
            var msg1 = new Message(payloadData1);

            string payload2 = "kafka 2.";
            byte[] payloadData2 = Encoding.UTF8.GetBytes(payload2);
            var msg2 = new Message(payloadData2);

            Assert.Throws<Kafka.Client.Exceptions.UnknownCodecException>(() => CompressionUtils.Compress(new List<Message>() { msg1, msg2 }, CompressionCodecs.NoCompressionCodec));
        }
    }
}
