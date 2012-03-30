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

using Kafka.Client.Exceptions;

namespace Kafka.Client.Messages
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.IO.Compression;
    using System.Reflection;
    using log4net;

    public static class CompressionUtils
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public static Message Compress(IEnumerable<Message> messages)
        {
            return Compress(messages, CompressionCodecs.DefaultCompressionCodec);
        }

        public static Message Compress(IEnumerable<Message> messages, CompressionCodecs compressionCodec)
        {
            switch (compressionCodec)
            {
                case CompressionCodecs.DefaultCompressionCodec:
                case CompressionCodecs.GZIPCompressionCodec:
                    using (MemoryStream outputStream = new MemoryStream())
                    {
                        using (GZipStream gZipStream = new GZipStream(outputStream, CompressionMode.Compress))
                        {
                            if (Logger.IsDebugEnabled)
                            {
                                Logger.DebugFormat(
                                    CultureInfo.CurrentCulture,
                                    "Allocating BufferedMessageSet of size = {0}",
                                    MessageSet.GetMessageSetSize(messages));
                            }

                            var bufferedMessageSet = new BufferedMessageSet(messages);
                            using (MemoryStream inputStream = new MemoryStream(bufferedMessageSet.SetSize))
                            {
                                bufferedMessageSet.WriteTo(inputStream);
                                inputStream.Position = 0;
                                try
                                {
                                    gZipStream.Write(inputStream.ToArray(), 0, inputStream.ToArray().Length);
                                    gZipStream.Close();
                                }
                                catch (IOException ex)
                                {
                                    Logger.Error("Error while writing to the GZIP stream", ex);
                                    throw;
                                }
                            }

                            Message oneCompressedMessage = new Message(outputStream.ToArray(), compressionCodec);
                            return oneCompressedMessage;
                        }
                    }

                default:
                    throw new UnknownCodecException(String.Format(CultureInfo.CurrentCulture, "Unknown Codec: {0}", compressionCodec));
            }
        }

        public static BufferedMessageSet Decompress(Message message)
        {
            switch (message.CompressionCodec)
            {
                case CompressionCodecs.DefaultCompressionCodec:
                case CompressionCodecs.GZIPCompressionCodec:
                    byte[] inputBytes = message.Payload;
                    using (MemoryStream outputStream = new MemoryStream())
                    {
                        using (MemoryStream inputStream = new MemoryStream(inputBytes))
                        {
                            using (GZipStream gzipInputStream = new GZipStream(inputStream, CompressionMode.Decompress))
                            {
                                try
                                {
                                    gzipInputStream.CopyTo(outputStream);
                                    gzipInputStream.Close();
                                }
                                catch (IOException ex)
                                {
                                    Logger.Error("Error while reading from the GZIP input stream", ex);
                                    throw;
                                }
                            }
                        }

                        return BufferedMessageSet.ParseFrom(outputStream.ToArray());
                    }

                default:
                    throw new UnknownCodecException(String.Format(CultureInfo.CurrentCulture, "Unknown Codec: {0}", message.CompressionCodec));
            }
        }
    }
}
