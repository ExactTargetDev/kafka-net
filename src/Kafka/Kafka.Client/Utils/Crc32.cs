using System;
using System.Security.Cryptography;

namespace Kafka.Client.Utils
{
    using System;
    using System.Security.Cryptography;

    public class Crc32
    {
        private const uint kCrcPoly = 0xEDB88320;
        private const uint kInitial = 0xFFFFFFFF;
        private static readonly uint[] Table;
        private const uint CRC_NUM_TABLES = 8;

        static Crc32()
        {
            unchecked
            {
                Table = new uint[256 * CRC_NUM_TABLES];
                uint i;
                for (i = 0; i < 256; i++)
                {
                    uint r = i;
                    for (int j = 0; j < 8; j++)
                        r = (r >> 1) ^ (kCrcPoly & ~((r & 1) - 1));
                    Table[i] = r;
                }
                for (; i < 256 * CRC_NUM_TABLES; i++)
                {
                    uint r = Table[i - 256];
                    Table[i] = Table[r & 0xFF] ^ (r >> 8);
                }
            }
        }

        private uint value;

        public Crc32()
        {
            Init();
        }

        /// <summary>
        /// Reset CRC
        /// </summary>
        public void Init()
        {
            value = kInitial;
        }

        public int Value
        {
            get { return (int)~value; }
        }

        public void UpdateByte(byte b)
        {
            value = (value >> 8) ^ Table[(byte)value ^ b];
        }

        public void Update(byte[] data, int offset, int count)
        {
            new ArraySegment<byte>(data, offset, count);     // check arguments
            if (count == 0) return;

            var table = Crc32.Table;        // important for performance!

            uint crc = value;

            for (; (offset & 7) != 0 && count != 0; count--)
                crc = (crc >> 8) ^ table[(byte)crc ^ data[offset++]];

            if (count >= 8)
            {
                /*
                 * Idea from 7-zip project sources (http://7-zip.org/sdk.html)
                 */

                int to = (count - 8) & ~7;
                count -= to;
                to += offset;

                while (offset != to)
                {
                    crc ^= (uint)(data[offset] + (data[offset + 1] << 8) + (data[offset + 2] << 16) + (data[offset + 3] << 24));
                    uint high = (uint)(data[offset + 4] + (data[offset + 5] << 8) + (data[offset + 6] << 16) + (data[offset + 7] << 24));
                    offset += 8;

                    crc = table[(byte)crc + 0x700]
                        ^ table[(byte)(crc >>= 8) + 0x600]
                        ^ table[(byte)(crc >>= 8) + 0x500]
                        ^ table[/*(byte)*/(crc >> 8) + 0x400]
                        ^ table[(byte)(high) + 0x300]
                        ^ table[(byte)(high >>= 8) + 0x200]
                        ^ table[(byte)(high >>= 8) + 0x100]
                        ^ table[/*(byte)*/(high >> 8) + 0x000];
                }
            }

            while (count-- != 0)
                crc = (crc >> 8) ^ table[(byte)crc ^ data[offset++]];

            value = crc;
        }

        static public int Compute(byte[] data, int offset, int size)
        {
            var crc = new Crc32();
            crc.Update(data, offset, size);
            return crc.Value;
        }

        static public int Compute(byte[] data)
        {
            return Compute(data, 0, data.Length);
        }

        static public int Compute(ArraySegment<byte> block)
        {
            return Compute(block.Array, block.Offset, block.Count);
        }
    }
}