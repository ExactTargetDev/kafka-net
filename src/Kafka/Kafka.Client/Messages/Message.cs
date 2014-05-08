using System;

namespace Kafka.Client.Messages
{
    using Kafka.Client.Common.Imported;

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
        /// <summary>
        /// The current offset and size for all the fixed-length fields
        /// </summary>
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

        /// <summary>
        /// The amount of overhead bytes in a message
        /// </summary>
        private const int MessageOverhead = KeyOffset + ValueSizeLength;

        /// <summary>
        /// The minimum valid size for the message header
        /// </summary>
        public const int MinHeaderSize = CrcLength + MagicLength + AttributesLength + KeySizeLength + ValueSizeLength;

        /// <summary>
        /// The current "magic" value
        /// </summary>
        private const byte CurrentMagicValue = 0;

        /// <summary>
        /// Specifies the mask for the compression code. 2 bits to hold the compression codec.
        /// 0 is reserved to indicate no compression
        /// </summary>
        private const byte CompressionCodeMask = 0x03;

        /// <summary>
        /// Compression code for uncompressed messages
        /// </summary>
        private const int NoCompression = 0;

        private readonly ByteBuffer buffer;

        public Message(ByteBuffer buffer)
        {
            this.buffer = buffer;
        }

        public ByteBuffer Buffer 
        { 
            get
            {
                return this.buffer;
            } 
        }

        /// <summary>
        ///  A constructor to create a Message
        /// </summary>
        /// <param name="bytes">The payload of the message</param>
        /// <param name="key">The key of the message (null, if none)</param>
        /// <param name="codec">The compression codec used on the contents of the message (if any)</param>
        /// <param name="payloadOffset">The offset into the payload array used to extract payload</param>
        /// <param name="payloadSize">The size of the payload to use</param>
        public Message(byte[] bytes, byte[] key, CompressionCodecs codec, int payloadOffset, int payloadSize)
        {
            this.buffer = ByteBuffer.Allocate(CrcLength +
                                      MagicLength +
                                      AttributesLength +
                                      KeySizeLength +
                                      ((key == null) ? 0 : key.Length) +
                                      ValueSizeLength +
                                      ((bytes == null)
                                           ? 0
                                           : (payloadSize >= 0 ? payloadSize : bytes.Length - payloadOffset)));
            this.buffer.Position = MagicOffset;
            this.buffer.Put(CurrentMagicValue);
            byte attributes = 0;
            if (codec != CompressionCodecs.NoCompressionCodec)
            {
                attributes =
                    Convert.ToByte(
                        attributes | (CompressionCodeMask & Messages.CompressionCodec.GetCompressionCodecValue(codec)));
            }

            this.buffer.Put(attributes);
            if (key == null)
            {
                this.buffer.PutInt(-1);
            }
            else
            {
                this.buffer.PutInt(key.Length);
                this.buffer.Put(key, 0, key.Length);
            }

            var size = (bytes == null)
                               ? -1
                               : (payloadSize >= 0) ? payloadSize : bytes.Length - payloadOffset;
            this.buffer.PutInt(size);
            if (bytes != null)
            {
                this.buffer.Write(bytes, payloadOffset, size);
            }

            this.buffer.Rewind();

            Utils.Util.WriteUnsignedInt(this.buffer, CrcOffset, this.ComputeChecksum());
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
            return Utils.Util.Crc32(this.buffer.Array, this.buffer.ArrayOffset() + MagicOffset, (int)this.buffer.Length - MagicOffset);
        }

        /// <summary>
        /// Retrieve the previously computed CRC for this message
        /// </summary>
        /// <returns></returns>
        public long Checksum 
        {
            get
            {
                return Utils.Util.ReadUnsingedInt(this.buffer, CrcOffset);
            }
        }

        /// <summary>
        /// Returns true if the CRC stored with the message matches the CRC computed off the message contents
        /// </summary>
        /// <returns></returns>
        public bool IsValid
        {
            get { return this.Checksum == this.ComputeChecksum(); }
        }

        /// <summary>
        /// Throw an InvalidMessageException if isValid is false for this message
        /// </summary>
        public void EnsureValid()
        {
            if (!this.IsValid)
            {
                throw new InvalidMessageException(string.Format("Message is corrupt (stored crc = {0}, computed crc = {1})", this.Checksum, this.ComputeChecksum()));
            }
        }

        /// <summary>
        /// The complete serialized size of this message in bytes (including CRC, header attributes, etc)
        /// </summary>
        /// <returns></returns>
        public int Size 
        {
            get { return this.buffer.Limit(); }
        }

        /// <summary>
        /// The length of the key in bytes
        /// </summary>
        /// <returns></returns>
        public int KeySize
        {
            get { return this.buffer.GetInt(KeySizeOffset); }
        }

        /// <summary>
        /// Does the message have a key?
        /// </summary>
        /// <returns></returns>
        public bool HasKey 
        {
            get { return this.KeySize >= 0; }
        }

        /// <summary>
        /// The position where the payload size is stored
        /// </summary>
        /// <returns></returns>
        private int PayloadSizeOffset
        {
            get { return KeyOffset + Math.Max(0, this.KeySize); }
        }

        /// <summary>
        /// The length of the message value in bytes
        /// </summary>
        /// <returns></returns>
        public int PayloadSize
        {
            get { return this.buffer.GetInt(this.PayloadSizeOffset); }
        }

        /// <summary>
        ///  Is the payload of this message null
        /// </summary>
        /// <returns></returns>
        public bool IsNull()
        {
            return this.PayloadSize < 0;
        }

        /// <summary>
        /// The magic version of this message
        /// </summary>
        /// <returns></returns>
        public byte Magic
        {
            get { return this.buffer.Get(MagicOffset); }
        }

        /// <summary>
        /// The attributes stored with this message
        /// </summary>
        /// <returns></returns>
        public byte Attributes
        {
            get { return this.buffer.Get(AttributesOffset); }
        }

        /// <summary>
        /// The compression codec used with this message
        /// </summary>
        /// <returns></returns>
        public CompressionCodecs CompressionCodec
        {
            get
            {
                return Messages.CompressionCodec.GetCompressionCodec(this.buffer.Get(AttributesOffset) &
                                                                          CompressionCodeMask);
            }
        }

        /// <summary>
        /// A ByteBuffer containing the content of the message
        /// </summary>
        public ByteBuffer Payload
        {
            get { return this.SliceDelimited(this.PayloadSizeOffset); }
        }

        public ByteBuffer Key
        {
            get { return this.SliceDelimited(KeySizeOffset);  }
        }

        /// <summary>
        /// Read a size-delimited byte buffer starting at the given offset
        /// </summary>
        /// <param name="start"></param>
        /// <returns></returns>
        private ByteBuffer SliceDelimited(int start)
        {
            var size = this.buffer.GetInt(start);
            if (size < 0)
            {
                return null;
            }
            else
            {
                var b = this.buffer.Duplicate();
                b.Position = start + 4;
                b = b.Slice();
                b.Limit(size);
                b.Rewind();
                return b;
            }
        }

        public override string ToString()
        {
            return string.Format("Magic: {0}, Attributes: {1}, Checksum: {2}, Payload: {3}, Key: {4}", this.Magic, this.Attributes, this.Checksum, this.Payload, this.Key);
        }

        protected bool Equals(Message other)
        {
            return this.buffer.Equals(other.Buffer);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            return obj.GetType() == this.GetType() && this.Equals((Message)obj);
        }

        public override int GetHashCode()
        {
            return this.buffer != null ? this.buffer.GetHashCode() : 0;
        }
    }
}