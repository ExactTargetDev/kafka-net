using System;
using System.IO;
using Kafka.Client.Common;
using Kafka.Client.Extensions;
using Kafka.Client.Serializers;

namespace Kafka.Client.Messages
{
    using System.Collections;

    /// <summary>
    /// A message. The format of an N byte message is the following:
    /// 1. 4 byte CRC32 of the message
    /// 2. 1 byte "magic" identifier to allow format changes, value is 2 currently
    /// 3. 1 byte "attributes" identifier to allow annotations on the message independent of the version (e.g. compression enabled, type of codec used)
    /// 4. 4 byte key length, containing length K
    /// 5. K byte key
    /// 6. 4 byte payload length, containing length V
    /// 7. V byte payload
    /// </summary>
    public class Message
    {
        private const int CrcOffset = 0;
        private const int CrcLength = 4;
        private const int MagicOffset = CrcOffset + CrcLength;
        private const int MagicLength = 1;
        private const int AttributesOffset = MagicOffset + MagicLength;
        private const int AttributesLength = 1;
        private const int KeySizeOffset = AttributesOffset + AttributesLength;
        private const int KeySizeLength = 4;
        private const int KeyOffset = KeySizeOffset + KeySizeLength;
        private const int ValueSizeLength = 4;


        private const int MessageOverhead = KeyOffset + ValueSizeLength;
        public const int MinHeaderSize = CrcLength + MagicLength + AttributesLength + KeySizeLength + ValueSizeLength;
        private const byte CurrentMagicValue = 0;
        private const byte CompressionCodeMask = 0x03;
        private const int NoCompression = 0;

        private readonly MemoryStream buffer;

        public Message(MemoryStream buffer)
        {
            this.buffer = buffer;
        }

        public MemoryStream Buffer { 
            get
            {
                return this.buffer;
            } 
        }


        public Message(byte[] bytes, byte[] key, CompressionCodecs codec, int payloadOffset, int payloadSize)
        {
            buffer = new MemoryStream(CrcLength +
                                      MagicLength +
                                      AttributesLength +
                                      KeySizeLength +
                                      ((key == null) ? 0 : key.Length) +
                                      ValueSizeLength +
                                      ((bytes == null)
                                           ? 0
                                           : ((payloadSize >= 0 ? payloadSize : bytes.Length - payloadOffset))));
            this.buffer.Position = MagicOffset;
            this.buffer.WriteByte(CurrentMagicValue);
            byte attributes = 0;
            if (codec != CompressionCodecs.NoCompressionCodec)
            {
                attributes =
                    Convert.ToByte(
                        attributes | (CompressionCodeMask & Messages.CompressionCodec.GetCompressionCodecValue(codec)));
            }
            this.buffer.WriteByte(attributes);
            if (key == null)
            {
                this.buffer.PutInt(-1);
            }
            else
            {
                this.buffer.PutInt(key.Length);
                this.buffer.Write(key, 0, key.Length);
            }
            var size = (bytes == null)
                               ? -1
                               : (payloadSize >= 0) ? payloadSize : bytes.Length - payloadOffset;
            this.buffer.PutInt(size);
            if (bytes != null)
            {
                this.buffer.Write(bytes, payloadOffset, size);
            }
            this.buffer.Position = 0;

            Utils.Util.WriteUnsignedInt(buffer, CrcOffset, this.ComputeChecksum());

        }

        public Message(byte[] bytes, byte[] key, CompressionCodecs codec) : this(bytes, key, codec, 0, -1)
        {
            
        }

        public Message(byte[] bytes, CompressionCodecs codec) : this(bytes, null, codec)
        {
            
        }

        public Message(byte[] bytes, byte[] key) : this(bytes, key, CompressionCodecs.NoCompressionCodec)
        {
            
        }

        public Message(byte[] bytes) : this(bytes, null, CompressionCodecs.NoCompressionCodec)
        {
            
        }

        /// <summary>
        /// Compute the checksum of the message from the message contents
        /// </summary>
        /// <returns></returns>
        public long ComputeChecksum()
        {
            return Utils.Util.Crc32(buffer.ToArray(), MagicOffset, (int)buffer.Length - MagicOffset);
        }

        /// <summary>
        /// Retrieve the previously computed CRC for this message
        /// </summary>
        /// <returns></returns>
        public long Checksum { get
        {
            return Utils.Util.ReadUnsingedInt(buffer, CrcOffset);
        }}

        /// <summary>
        /// Returns true if the crc stored with the message matches the crc computed off the message contents
        /// </summary>
        /// <returns></returns>
        public bool IsValid
        {
            get { return Checksum == ComputeChecksum(); }
        }


        /// <summary>
        /// Throw an InvalidMessageException if isValid is false for this message
        /// </summary>
        public void EnsureValid()
        {
            if (!IsValid)
            {
                throw new InvalidMessageException(String.Format("Message is corrupt (stored crc = {0}, computed crc = {1})", Checksum, ComputeChecksum()));
            }
        }

        /// <summary>
        /// The complete serialized size of this message in bytes (including crc, header attributes, etc)
        /// </summary>
        /// <returns></returns>
        public int Size {
            get { return (int)this.buffer.Length; }
        }

        /// <summary>
        /// The length of the key in bytes
        /// </summary>
        /// <returns></returns>
        public int KeySize { get { return buffer.GetInt(Message.KeySizeOffset); } }

        /// <summary>
        /// Does the message have a key?
        /// </summary>
        /// <returns></returns>
        public bool HasKey {
            get { return KeySize >= 0; }
        }

        /// <summary>
        /// The position where the payload size is stored
        /// </summary>
        /// <returns></returns>
        private int PayloadSizeOffset
        {
            get { return KeyOffset + Math.Max(0, KeySize); }
        }

        /// <summary>
        /// The length of the message value in bytes
        /// </summary>
        /// <returns></returns>
        public int PayloadSize
        {
            get { return buffer.GetInt(PayloadSizeOffset); }
        }

        /// <summary>
        ///  Is the payload of this message null
        /// </summary>
        /// <returns></returns>
        public bool IsNull()
        {
            return PayloadSize < 0;
        }

        /// <summary>
        /// The magic version of this message
        /// </summary>
        /// <returns></returns>
        public byte Magic
        {
            get { return this.buffer.GetBuffer()[MagicOffset]; }
        }

        /// <summary>
        /// The attributes stored with this message
        /// </summary>
        /// <returns></returns>
        public byte Attributes
        {
            get { return this.buffer.GetBuffer()[AttributesOffset]; }
        }

        /// <summary>
        /// The compression codec used with this message
        /// </summary>
        /// <returns></returns>
        public CompressionCodecs CompressionCodec
        {
            get
            {
                return Messages.CompressionCodec.GetCompressionCodec(buffer.GetBuffer()[AttributesOffset] &
                                                                          CompressionCodeMask);
            }
        }

        /// <summary>
        /// A ByteBuffer containing the content of the message
        /// </summary>
        public MemoryStream Payload
        {
            get { return this.SliceDelimited(PayloadSizeOffset); }
        }

        public MemoryStream Key
        {
            get { return this.SliceDelimited(KeySizeOffset);  }
        }

        /// <summary>
        /// Read a size-delimited byte buffer starting at the given offset
        /// </summary>
        /// <param name="start"></param>
        /// <returns></returns>
        private MemoryStream SliceDelimited(int start)
        {
            int size = buffer.GetInt(start);
            if (size < 0)
            {
                return null;
            }
            return new MemoryStream(buffer.GetBuffer(), start + 4, size, false);
        }

        public override string ToString()
        {
            return string.Format("Magic: {0}, Attributes: {1}, Checksum: {2}, Payload: {3}, Key: {4}", Magic, Attributes, Checksum, Payload, Key);
        }

        protected bool Equals(Message other)
        {
            return StructuralComparisons.StructuralEqualityComparer.Equals(buffer, other.buffer);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Message) obj);
        }

        public override int GetHashCode()
        {
            return this.buffer != null ? this.buffer.GetHashCode() : 0;
        }

    }
}