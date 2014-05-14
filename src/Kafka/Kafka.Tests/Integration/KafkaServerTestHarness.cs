namespace Kafka.Tests.Integration
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    using Kafka.Client.Common;
    using Kafka.Tests.Custom.Server;
    using Kafka.Tests.Utils;
    using Kafka.Tests.Zk;

    public abstract class KafkaServerTestHarness : ZooKeeperTestHarness
    {
        protected List<TempKafkaConfig> Configs { get; private set; }

        protected abstract List<TempKafkaConfig> CreateConfigs();

        protected List<Process> Servers { get; private set; } 

        protected KafkaServerTestHarness()
        {
            this.Configs = this.CreateConfigs();
            if (this.Configs.Count <= 0)
            {
                throw new KafkaException("Must suply at least one server config.");
            }

            this.Servers = this.Configs.Select(this.StartServer).ToList();
        }

        private Process StartServer(TempKafkaConfig config)
        {
            return KafkaRunClassHelper.Run(KafkaRunClassHelper.KafkaServerMainClass, config.ConfigLocation); 
        }

        public override void Dispose()
        {
            base.Dispose();
            foreach (var process in this.Servers)
            {
                try
                {
                    process.Kill();
                }
                catch
                {
                }
            }
            foreach (var serverConfig in this.Configs)
            {
                serverConfig.Dispose();
            }
        }
    }
}