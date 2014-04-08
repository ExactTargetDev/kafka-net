using System;
using System.Security.Cryptography;

namespace Kafka.Client.Utils
{
        /// <summary>
        /// From http://damieng.com/blog/2006/08/08/calculating_crc32_in_c_and_net
        /// </summary>
        internal class Crc32 : HashAlgorithm
        {
            public const uint DefaultPolynomial = 0xedb88320;
            public const uint DefaultSeed = 0xffffffff;

            private uint hash;
            private uint seed;
            private uint[] table;
            private static UInt32[] defaultTable;

            public Crc32()
            {
                table = InitializeTable(DefaultPolynomial);
                seed = DefaultSeed;
                this.Initialize();
            }

            public Crc32(uint polynomial, uint seed)
            {
                table = InitializeTable(polynomial);
                this.seed = seed;
                this.Initialize();
            }

            public override void Initialize()
            {
                hash = seed;
            }

            protected override void HashCore(byte[] buffer, int start, int length)
            {
                hash = CalculateHash(this.table, hash, buffer, start, length);
            }

            protected override byte[] HashFinal()
            {
                byte[] hashBuffer = this.UInt32ToBigEndianBytes(~hash);
                this.HashValue = hashBuffer;
                return hashBuffer;
            }

            public override int HashSize
            {
                get { return 32; }
            }

            public static byte[] Compute(byte[] bytes)
            {
                var hasher = new Crc32();
                byte[] hash = hasher.ComputeHash(bytes);
                return hash;
            }

            private static uint[] InitializeTable(uint polynomial)
            {
                if (polynomial == DefaultPolynomial && defaultTable != null)
                    return defaultTable;

                uint[] createTable = new uint[256];
                for (int i = 0; i < 256; i++)
                {
                    uint entry = (uint)i;
                    for (int j = 0; j < 8; j++)
                        if ((entry & 1) == 1)
                            entry = (entry >> 1) ^ polynomial;
                        else
                            entry = entry >> 1;
                    createTable[i] = entry;
                }

                if (polynomial == DefaultPolynomial)
                    defaultTable = createTable;

                return createTable;
            }

            private static uint CalculateHash(uint[] table, uint seed, byte[] buffer, int start, int size)
            {
                uint crc = seed;
                for (var i = start; i < size; i++)
                    unchecked
                    {
                        crc = (crc >> 8) ^ table[buffer[i] ^ crc & 0xff];
                    }
                return crc;
            }

            private byte[] UInt32ToBigEndianBytes(uint x)
            {
                return new [] {
                    (byte)((x >> 24) & 0xff),
                    (byte)((x >> 16) & 0xff),
                    (byte)((x >> 8) & 0xff),
                    (byte)(x & 0xff)
		        };
            }
        }
}