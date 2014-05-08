namespace Kafka.Client.Messages
{
    public class MessageAndOffset
    {
        public Message Message { get; private set; }

        public long Offset { get; private set; }

        public MessageAndOffset(Message message, long offset)
        {
            this.Message = message;
            this.Offset = offset;
        }

        /// <summary>
        /// Compute the offset of the next message in the log
        /// </summary>
        /// <returns></returns>
        /// 
        public long NextOffset
        {
            get
            {
                return this.Offset + 1;
            }
        }

        protected bool Equals(MessageAndOffset other)
        {
            return Equals(this.Message, other.Message) && this.Offset == other.Offset;
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

            return Equals((MessageAndOffset)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((this.Message != null ? this.Message.GetHashCode() : 0) * 397) ^ this.Offset.GetHashCode();
            }
        }
    }
}