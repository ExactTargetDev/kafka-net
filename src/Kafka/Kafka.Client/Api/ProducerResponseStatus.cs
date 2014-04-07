namespace Kafka.Client.Api
{
    public class ProducerResponseStatus
    {
        public short Error { get; private set; }

        public long Offset { get; private set; }

        public ProducerResponseStatus(short error, long offset)
        {
            this.Error = error;
            this.Offset = offset;
        }
    }
}