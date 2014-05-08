namespace Kafka.Client.Common.Imported
{
    using System;
    using System.IO;
    using System.Text;

    public abstract class ByteBuffer : Stream
    {
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

            return new HeapByteBuffer(capacity, capacity);
        }

        public static ByteBuffer Wrap(byte[] array, int offset, int length)
        {
            try
            {
                return new HeapByteBuffer(array, offset, length);
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

        public abstract ByteBuffer Slice();

        public abstract ByteBuffer Duplicate();

        public abstract byte Get();

        public abstract ByteBuffer Put(byte b);

        public abstract byte Get(int index);

        public abstract ByteBuffer Put(int index, byte b);

        public virtual ByteBuffer Get(byte[] dst, int offset, int length)
        {
            CheckBounds(offset, length, dst.Length);
            if (length > this.Remaining())
            {
                throw new ArgumentOutOfRangeException();
            }
                
            int end = offset + length;
            for (int i = offset; i < end; i++)
            {
                dst[i] = this.Get();
            }

            return this;
        }

        public ByteBuffer Get(byte[] dst)
        {
            return this.Get(dst, 0, dst.Length);
        }

        public virtual ByteBuffer Put(ByteBuffer src)
        {
            if (src == this)
            {
                throw new ArgumentException();
            }
                
            int n = src.Remaining();
            if (n > this.Remaining())
            {
                throw new ArgumentOutOfRangeException();
            }

            for (int i = 0; i < n; i++)
            {
                this.Put(src.Get());
            }

            return this;
        }

        public virtual ByteBuffer Put(byte[] src, int offset, int length)
        {
            CheckBounds(offset, length, src.Length);
            if (length > this.Remaining())
            {
                throw new IndexOutOfRangeException();
            }

            int end = offset + length;
            for (int i = offset; i < end; i++)
            {
                this.Put(src[i]);
            }
                
            return this;
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

        public abstract ByteBuffer Compact();

        public abstract bool IsDirect();

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

        internal abstract byte _Get(int i);

        internal abstract void _Put(int i, byte b);

        public abstract short GetShort();

        public abstract ByteBuffer PutShort(short value);

        public abstract short GetShort(int index);

        public abstract ByteBuffer PutShort(int index, short value);

        public abstract int GetInt();

        public abstract ByteBuffer PutInt(int value);

        public abstract int GetInt(int index);

        public abstract ByteBuffer PutInt(int index, int value);

        public abstract long GetLong();

        public abstract ByteBuffer PutLong(long value);

        public abstract long GetLong(int index);

        public abstract ByteBuffer PutLong(int index, long value);

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
    }
}