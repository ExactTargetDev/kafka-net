namespace Kafka.Client.Common.Imported
{
    using System;
    using System.IO;
    using System.Net;

    internal class HeapByteBuffer : ByteBuffer
    {
        internal HeapByteBuffer(int cap, int lim)
            : base(-1, 0, lim, cap, new byte[cap], 0)
        {
        }

        internal HeapByteBuffer(byte[] buf, int off, int len)
            : base(-1, off, off + len, buf.Length, buf, 0)
        {
        }

        protected HeapByteBuffer(byte[] buf, int mark, int pos, int lim, int cap, int off)
            : base(mark, pos, lim, cap, buf, off)
        {
        }

        public override ByteBuffer Slice()
        {
            return new HeapByteBuffer(hb,
                                            -1,
                                            0,
                                            this.Remaining(),
                                            this.Remaining(),
                                            (int)this.Position + offset);
        }

        public override ByteBuffer Duplicate()
        {
            return new HeapByteBuffer(hb,
                                            this.MarkValue(),
                                            (int)this.Position,
                                            this.Limit(),
                                            this.Capacity(),
                                            offset);
        }

        protected int Ix(int i)
        {
            return i + offset;
        }

        public override byte Get()
        {
            return hb[Ix(NextGetIndex())];
        }

        public override byte Get(int i)
        {
            return hb[Ix(CheckIndex(i))];
        }

        public override ByteBuffer Get(byte[] dst, int offset, int length)
        {
            CheckBounds(offset, length, dst.Length);
            if (length > Remaining())
                throw new IndexOutOfRangeException();

            Buffer.BlockCopy(hb, Ix((int)Position), dst, offset, length);
            this.Position = this.Position + length;
            return this;
        }

        public override bool IsDirect()
        {
            return false;
        }

        public bool IsReadOnly()
        {
            return false;
        }

        public override ByteBuffer Put(byte x)
        {
            hb[Ix(NextPutIndex())] = x;
            return this;
        }

        public override ByteBuffer Put(int i, byte x)
        {
            hb[Ix(CheckIndex(i))] = x;
            return this;
        }

        public override ByteBuffer Put(byte[] src, int offset, int length)
        {
            CheckBounds(offset, length, src.Length);
            if (length > Remaining())
                throw new ArgumentOutOfRangeException();
            Buffer.BlockCopy(src, offset, hb, this.Ix((int)Position), length);
            Position = Position + length;
            return this;
        }

        public ByteBuffer Put(ByteBuffer src)
        {
            if (src == this)
                throw new ArgumentNullException("src");
            int n = src.Remaining();
            if (n > Remaining())
            {
                throw new IndexOutOfRangeException();
            }
            for (int i = 0; i < n; i++)
            {
                Put(src.Get());
            }
            return this;
        }

        public override ByteBuffer Compact()
        {
            Buffer.BlockCopy(hb, this.Ix((int)Position), hb, this.Ix(0), this.Remaining());
            this.Position = this.Remaining();
            Limit(Capacity());
            DiscardMark();
            return this;
        }

        internal override byte _Get(int i)
        {
            return hb[i];
        }

        internal override void _Put(int i, byte b)
        {
            hb[i] = b;
        }

        public override short GetShort()
        {
            int startIndex = this.Ix(this.NextGetIndex(2));

            var value =(short)(hb[startIndex] | hb[startIndex + 1] << 8);

            return IPAddress.NetworkToHostOrder(value);
        }

        public override short GetShort(int i)
        {
            int startIndex = this.Ix(this.CheckIndex(i,2));

            var value = (short)(hb[startIndex] | hb[startIndex + 1] << 8);

            return IPAddress.NetworkToHostOrder(value);
        }

        public override ByteBuffer PutShort(short x)
        {
            x = IPAddress.HostToNetworkOrder(x);

            var startIndex = this.Ix(this.NextPutIndex(2));
            this.hb[startIndex] = (byte)x;
            this.hb[startIndex + 1] = (byte)(x >> 8);
            return this;
        }

        public override ByteBuffer PutShort(int i, short x)
        {
            x = IPAddress.HostToNetworkOrder(x);

            var startIndex = this.Ix(this.CheckIndex(i, 2));
            this.hb[startIndex] = (byte)x;
            this.hb[startIndex + 1] = (byte)(x >> 8);
            return this;
        }

        public override int GetInt()
        {
            int startIndex = this.Ix(this.NextGetIndex(4));

            var value = hb[startIndex] | hb[startIndex + 1] << 8 | hb[startIndex + 2] << 16 | hb[startIndex + 3] << 24;

            return IPAddress.NetworkToHostOrder(value);
        }

        public override int GetInt(int i)
        {
            int startIndex = this.Ix(this.CheckIndex(i, 4));

            var value = hb[startIndex] | hb[startIndex + 1] << 8 | hb[startIndex + 2] << 16 | hb[startIndex + 3] << 24;

            return IPAddress.NetworkToHostOrder(value);
        }

        public override ByteBuffer PutInt(int x)
        {
            x = IPAddress.HostToNetworkOrder(x);

            var startIndex = this.Ix(this.NextPutIndex(4));
            this.hb[startIndex] = (byte)x;
            this.hb[startIndex + 1] = (byte)(x >> 8);
            this.hb[startIndex + 2] = (byte)(x >> 16);
            this.hb[startIndex + 3] = (byte)(x >> 24);
            return this;
        }

        public override ByteBuffer PutInt(int i, int x)
        {
            x = IPAddress.HostToNetworkOrder(x);

            var startIndex = this.Ix(this.CheckIndex(i,4));
            this.hb[startIndex] = (byte)x;
            this.hb[startIndex + 1] = (byte)(x >> 8);
            this.hb[startIndex + 2] = (byte)(x >> 16);
            this.hb[startIndex + 3] = (byte)(x >> 24);
            return this;
        }

        public override long GetLong()
        {
            int startIndex = this.Ix(this.NextGetIndex(8));

            var lo = (uint)(hb[startIndex] | hb[startIndex + 1] << 8 |
                             hb[startIndex + 2] << 16 | hb[startIndex + 3] << 24);
            var hi = (uint)(hb[startIndex + 4] | hb[startIndex + 5] << 8 |
                             hb[startIndex + 6] << 16 | hb[startIndex + 7] << 24);
            return IPAddress.NetworkToHostOrder((long)((ulong)hi) << 32 | lo);
        }

        public override long GetLong(int i)
        {
            int startIndex = this.Ix(this.CheckIndex(i,8));

            var lo = (uint)(hb[startIndex] | hb[startIndex + 1] << 8 |
                             hb[startIndex + 2] << 16 | hb[startIndex + 3] << 24);
            var hi = (uint)(hb[startIndex + 4] | hb[startIndex + 5] << 8 |
                             hb[startIndex + 6] << 16 | hb[startIndex + 7] << 24);
            return IPAddress.NetworkToHostOrder((long)((ulong)hi) << 32 | lo);
        }


        public override ByteBuffer PutLong(long x)
        {
            x = IPAddress.HostToNetworkOrder(x);

            var startIndex = this.Ix(this.NextPutIndex(8));
            this.hb[startIndex] = (byte)x;
            this.hb[startIndex + 1] = (byte)(x >> 8);
            this.hb[startIndex + 2] = (byte)(x >> 16);
            this.hb[startIndex + 3] = (byte)(x >> 24);
            this.hb[startIndex + 4] = (byte)(x >> 32);
            this.hb[startIndex + 5] = (byte)(x >> 40);
            this.hb[startIndex + 6] = (byte)(x >> 48);
            this.hb[startIndex + 7] = (byte)(x >> 56);
            return this;
        }

        public override ByteBuffer PutLong(int i, long x)
        {
            x = IPAddress.HostToNetworkOrder(x);

            var startIndex = this.Ix(this.CheckIndex(i, 8));
            this.hb[startIndex] = (byte)x;
            this.hb[startIndex + 1] = (byte)(x >> 8);
            this.hb[startIndex + 2] = (byte)(x >> 16);
            this.hb[startIndex + 3] = (byte)(x >> 24);
            this.hb[startIndex + 4] = (byte)(x >> 32);
            this.hb[startIndex + 5] = (byte)(x >> 40);
            this.hb[startIndex + 6] = (byte)(x >> 48);
            this.hb[startIndex + 7] = (byte)(x >> 56);
            return this;
        }

    }
}