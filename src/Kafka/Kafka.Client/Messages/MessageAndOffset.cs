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
    }
}