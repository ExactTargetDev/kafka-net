namespace Kafka.Client.Common.Imported
{
	using log4net;
	using System;
	using System.IO;
	using System.Net;
	using System.Reflection;
	using System.Text;

	public class ByteBuffer : Stream
    {
		protected static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

		#region Buffer

		private int mark = -1;

        private int position;

        private int limit;

        private int capacity;

        private ByteBuffer(int mark, int pos, int lim, int cap)
        {       // package-private
            if (cap < 0)
            {
                throw new ArgumentException("Negative capacity: " + cap, "cap");
            }

            this.capacity = cap;
            this.Limit(lim);
            this.Position = pos;
            if (mark >= 0)
            {
                if (mark > pos)
                {
                    throw new ArgumentException("mark > position: (" + mark + " > " + pos + ")");
                }

                this.mark = mark;
            }
        }

        public int Capacity()
        {
            return this.capacity;
        }

        public int Limit()
        {
            return this.limit;
        }

        public ByteBuffer Limit(int newLimit)
        {
            if ((newLimit > this.capacity) || (newLimit < 0))
            {
                throw new ArgumentException();
            }
                
            this.limit = newLimit;
            if (this.position > this.limit)
            {
                this.position = this.limit;
            }

            if (this.mark > this.limit)
            {
                this.mark = -1;
            }

            return this;
        }

        public ByteBuffer Mark()
        {
            this.mark = this.position;
            return this;
        }

        public ByteBuffer Reset()
        {
            int m = this.mark;
            if (m < 0)
            {
                throw new Exception();
            }
                
            this.position = m;
            return this;
        }

        public ByteBuffer Clear()
        {
            this.position = 0;
            this.limit = this.capacity;
            this.mark = -1;
            return this;
        }

        public ByteBuffer Flip()
        {
            this.limit = this.position;
            this.position = 0;
            this.mark = -1;
            return this;
        }

        public ByteBuffer Rewind()
        {
            this.position = 0;
            this.mark = -1;
            return this;
        }

        public int Remaining()
        {
            return this.limit - this.position;
        }

        public bool HasRemaining()
        {
            return this.position < this.limit;
        }

        internal int NextGetIndex()
        {
            if (this.position >= this.limit)
            {
                throw new ArgumentOutOfRangeException();
            }

            return this.position++;
        }

        internal int NextGetIndex(int nb)
        {
            if (this.limit - this.position < nb)
            {
                throw new ArgumentOutOfRangeException();
            }

            int p = this.position;
            this.position += nb;
            return p;
        }

        internal int NextPutIndex()
        {
            if (this.position >= this.limit)
            {
                throw new ArgumentOutOfRangeException();
            }

            return this.position++;
        }

        internal int NextPutIndex(int nb)
        {
            if (this.limit - this.position < nb)
            {
                throw new ArgumentOutOfRangeException();
            }

            int p = this.position;
            this.position += nb;
            return p;
        }

        internal int CheckIndex(int i)
        {
            if ((i < 0) || (i >= this.limit))
            {
                throw new ArgumentOutOfRangeException();
            }

            return i;
        }

        internal int CheckIndex(int i, int nb)
        {
            if ((i < 0) || (nb > this.limit - i))
            {
                throw new ArgumentOutOfRangeException();
            }
                
            return i;
        }

        internal int MarkValue()
        {
            return this.mark;
        }

        internal void Truncate()
        {
            this.mark = -1;
            this.position = 0;
            this.limit = 0;
            this.capacity = 0;
        }

        internal void DiscardMark()
        {
            this.mark = -1;
        }

        internal static void CheckBounds(int off, int len, int size)
        {
            if ((off | len | (off + len) | (size - (off + len))) < 0)
            {
                throw new IndexOutOfRangeException();
            }
        }

        #endregion

        #region ByteBuffer

        protected byte[] hb;

        protected int offset;

        private bool isReadOnly;

        internal ByteBuffer(int mark, int pos, int lim, int cap, byte[] hb, int offset)
            : this(mark, pos, lim, cap)
        {
            this.hb = hb;
            this.offset = offset;
        }

        public static ByteBuffer Allocate(int capacity)
        {
            if (capacity < 0)
            {
                throw new ArgumentException();
            }

            return new ByteBuffer(capacity, capacity);
        }

        public static ByteBuffer Wrap(byte[] array, int offset, int length)
        {
            try
            {
                return new ByteBuffer(array, offset, length);
            }
            catch (ArgumentException)
            {
                throw new IndexOutOfRangeException();
            }
        }

        public static ByteBuffer Wrap(byte[] array)
        {
            return Wrap(array, 0, array.Length);
        }

        public ByteBuffer Get(byte[] dst)
        {
            return this.Get(dst, 0, dst.Length);
        }

        public ByteBuffer Put(byte[] src)
        {
            return this.Put(src, 0, src.Length);
        }

        public bool HasArray()
        {
            return (this.hb != null) && !this.isReadOnly;
        }

        public byte[] Array
        {
            get
            {
                return this.hb;
            }
        }

        public int ArrayOffset()
        {
            if (this.hb == null)
            {
                throw new InvalidOperationException();
            }

            if (this.isReadOnly)
            {
                throw new UnauthorizedAccessException();
            }
                
            return this.offset;
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(this.GetType().Name);
            sb.Append("[pos=");
            sb.Append(this.Position);
            sb.Append(" lim=");
            sb.Append(this.Limit());
            sb.Append(" cap=");
            sb.Append(this.Capacity());
            sb.Append("]");
            return sb.ToString();
        }

        public override int GetHashCode()
        {
            return this.hb != null ? this.hb.GetHashCode() : 0;
        }

        protected bool Equals(ByteBuffer other)
        {
            if (this.Remaining() != other.Remaining())
            {
                return false;
            }

            int p = (int)this.Position;

            for (int i = this.Limit() - 1, j = other.Limit() - 1; i >= p; i--, j--)
            {
                if (!Equals(this.Get(i), other.Get(j)))
                {
                    return false;
                }
            }

            return true;
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

            if (obj.GetType() != this.GetType())
            {
                return false;
            }

            return this.Equals((ByteBuffer)obj);
        }

        #endregion

        #region Stream

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            this.Limit((int)value);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var toRead = Math.Min(count, this.Remaining());
            this.Get(buffer, offset, toRead);
            return toRead;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            this.Put(buffer, offset, count);
        }

        public override bool CanRead
        {
            get
            {
                return this.isReadOnly == false;
            }
        }

        public override bool CanSeek
        {
            get
            {
                return true;
            }
        }

        public override bool CanWrite
        {
            get
            {
                return true;
            }
        }

        public override long Length
        {
            get
            {
                return this.limit;
            }
        }

        public override long Position
        {
            get
            {
                return this.position;
            }

            set
            {
                if ((value > this.limit) || (value < 0))
                {
                    throw new ArgumentException();
                }

                this.position = (int)value;
                if (this.mark > this.position)
                {
                    this.mark = -1;
                }
            }
        }
        #endregion


        internal ByteBuffer(int cap, int lim)
            : this(-1, 0, lim, cap, new byte[cap], 0)
        {
        }

        internal ByteBuffer(byte[] buf, int off, int len)
            : this(-1, off, off + len, buf.Length, buf, 0)
        {
        }

        protected ByteBuffer(byte[] buf, int mark, int pos, int lim, int cap, int off)
            : this(mark, pos, lim, cap, buf, off)
        {
        }

        public ByteBuffer Slice()
        {
            return new ByteBuffer(
                hb,
                -1,
                0,
                this.Remaining(),
                this.Remaining(),
                (int)this.Position + offset);
        }

        public ByteBuffer Duplicate()
        {
            return new ByteBuffer(
                hb,
                this.MarkValue(),
                (int)this.Position,
                this.Limit(),
                this.Capacity(),
                offset);
        }

        protected int Ix(int i)
        {
            return i + this.offset;
        }

        public byte Get()
        {
            return this.hb[this.Ix(this.NextGetIndex())];
        }

        public byte Get(int i)
        {
            return this.hb[this.Ix(this.CheckIndex(i))];
        }

        public ByteBuffer Get(byte[] dst, int offset, int length)
        {
            CheckBounds(offset, length, dst.Length);
            if (length > this.Remaining())
            {
                throw new IndexOutOfRangeException();
            }

            Buffer.BlockCopy(this.hb, this.Ix((int)Position), dst, offset, length);
            this.Position = this.Position + length;
            return this;
        }

        public bool IsDirect()
        {
            return false;
        }

        public bool IsReadOnly()
        {
            return false;
        }

        public ByteBuffer Put(byte x)
        {
            this.hb[this.Ix(this.NextPutIndex())] = x;
            return this;
        }

        public ByteBuffer Put(int i, byte x)
        {
            this.hb[this.Ix(this.CheckIndex(i))] = x;
            return this;
        }

        public ByteBuffer Put(byte[] src, int offset, int length)
        {
            CheckBounds(offset, length, src.Length);
            if (length > this.Remaining())
            {
                throw new ArgumentOutOfRangeException();
            }
                
            Buffer.BlockCopy(src, offset, this.hb, this.Ix((int)Position), length);
            this.Position = this.Position + length;
            return this;
        }

        public ByteBuffer Put(ByteBuffer src)
        {
            if (ReferenceEquals(src, this))
            {
                throw new ArgumentNullException("src");
            }
                
            int n = src.Remaining();
            if (n > this.Remaining())
            {
                throw new IndexOutOfRangeException();
            }

            Buffer.BlockCopy(src.hb, src.Ix(src.position), this.hb, this.Ix(this.position), n);
            this.position += n;
            src.position += n;

            return this;
        }

        public ByteBuffer Compact()
        {
            Buffer.BlockCopy(this.hb, this.Ix((int)Position), this.hb, this.Ix(0), this.Remaining());
            this.Position = this.Remaining();
            this.Limit(this.Capacity());
            this.DiscardMark();
            return this;
        }

        internal byte _Get(int i)
        {
            return this.hb[i];
        }

        internal void _Put(int i, byte b)
        {
            this.hb[i] = b;
        }

        public short GetShort()
        {
            int startIndex = this.Ix(this.NextGetIndex(2));

            var value = (short)(hb[startIndex] | hb[startIndex + 1] << 8);

            return IPAddress.NetworkToHostOrder(value);
        }

        public short GetShort(int i)
        {
            int startIndex = this.Ix(this.CheckIndex(i, 2));

            var value = (short)(hb[startIndex] | hb[startIndex + 1] << 8);

            return IPAddress.NetworkToHostOrder(value);
        }

        public ByteBuffer PutShort(short x)
        {
            x = IPAddress.HostToNetworkOrder(x);

            var startIndex = this.Ix(this.NextPutIndex(2));
            this.hb[startIndex] = (byte)x;
            this.hb[startIndex + 1] = (byte)(x >> 8);
            return this;
        }

        public ByteBuffer PutShort(int i, short x)
        {
            x = IPAddress.HostToNetworkOrder(x);

            var startIndex = this.Ix(this.CheckIndex(i, 2));
            this.hb[startIndex] = (byte)x;
            this.hb[startIndex + 1] = (byte)(x >> 8);
            return this;
        }

        public int GetInt()
        {
            int startIndex = this.Ix(this.NextGetIndex(4));

            var value = hb[startIndex] | hb[startIndex + 1] << 8 | hb[startIndex + 2] << 16 | hb[startIndex + 3] << 24;

            return IPAddress.NetworkToHostOrder(value);
        }

        public int GetInt(int i)
        {
            int startIndex = this.Ix(this.CheckIndex(i, 4));

            var value = hb[startIndex] | hb[startIndex + 1] << 8 | hb[startIndex + 2] << 16 | hb[startIndex + 3] << 24;

            return IPAddress.NetworkToHostOrder(value);
        }

        public ByteBuffer PutInt(int x)
        {
            x = IPAddress.HostToNetworkOrder(x);

            var startIndex = this.Ix(this.NextPutIndex(4));
            this.hb[startIndex] = (byte)x;
            this.hb[startIndex + 1] = (byte)(x >> 8);
            this.hb[startIndex + 2] = (byte)(x >> 16);
            this.hb[startIndex + 3] = (byte)(x >> 24);
            return this;
        }

        public ByteBuffer PutInt(int i, int x)
        {
            x = IPAddress.HostToNetworkOrder(x);

            var startIndex = this.Ix(this.CheckIndex(i, 4));
            this.hb[startIndex] = (byte)x;
            this.hb[startIndex + 1] = (byte)(x >> 8);
            this.hb[startIndex + 2] = (byte)(x >> 16);
            this.hb[startIndex + 3] = (byte)(x >> 24);
            return this;
        }

        public long GetLong()
        {
            int startIndex = this.Ix(this.NextGetIndex(8));

            var lo = (uint)(hb[startIndex] | hb[startIndex + 1] << 8 |
                             hb[startIndex + 2] << 16 | hb[startIndex + 3] << 24);
            var hi = (uint)(hb[startIndex + 4] | hb[startIndex + 5] << 8 |
                             hb[startIndex + 6] << 16 | hb[startIndex + 7] << 24);
            return IPAddress.NetworkToHostOrder((long)((ulong)hi) << 32 | lo);
        }

        public long GetLong(int i)
        {
            int startIndex = this.Ix(this.CheckIndex(i, 8));

            var lo = (uint)(hb[startIndex] | hb[startIndex + 1] << 8 |
                             hb[startIndex + 2] << 16 | hb[startIndex + 3] << 24);
            var hi = (uint)(hb[startIndex + 4] | hb[startIndex + 5] << 8 |
                             hb[startIndex + 6] << 16 | hb[startIndex + 7] << 24);
            return IPAddress.NetworkToHostOrder((long)((ulong)hi) << 32 | lo);
        }

        public ByteBuffer PutLong(long x)
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

        public ByteBuffer PutLong(int i, long x)
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