namespace Kafka.Client.Api
{
    public class PartitionFetchInfo
    {
        public long Offset { get; private set; }

        public int FetchSize { get; private set; }

        public PartitionFetchInfo(long offset, int fetchSize)
        {
            this.Offset = offset;
            this.FetchSize = fetchSize;
        }

        protected bool Equals(PartitionFetchInfo other)
        {
            return this.Offset == other.Offset && this.FetchSize == other.FetchSize;
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
            return Equals((PartitionFetchInfo)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (this.Offset.GetHashCode() * 397) ^ this.FetchSize;
            }
        }
    }
}