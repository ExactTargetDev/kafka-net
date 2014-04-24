namespace Kafka.Client.Utils
{
    public class ZKGroupTopicDirs : ZKGroupDirs
    {
        public string Topic { get; private set; }

        public ZKGroupTopicDirs(string @group, string topic)
            : base(@group)
        {
            this.Topic = topic;
        }

        public string ConsumerOffsetDir
        {
            get
            {
                return this.ConsumerGroupDir + "/offsets/" + this.Topic;
            }
        }

        public string ConsumerOwnerDir
        {
            get
            {
                return this.ConsumerGroupDir + "/owners/" + this.Topic;    
            }
        }
    }
}