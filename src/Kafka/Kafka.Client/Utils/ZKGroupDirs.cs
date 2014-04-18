namespace Kafka.Client.Utils
{
    public class ZKGroupDirs
    {
        public string Group { get; set; }

        public ZKGroupDirs(string @group)
        {
            this.Group = @group;
        }

        public string ConsumerDir
        {
            get
            {
                return ZkUtils.ConsumersPath;
            }
        }

        public string ConsumerGroupDir
        {
            get
            {
                return ConsumerDir + "/" + Group;
            }
        }

        public string ConsumerRegistryDir
        {
            get
            {
                return this.ConsumerDir + "/ids";
            }
        }
    }
}