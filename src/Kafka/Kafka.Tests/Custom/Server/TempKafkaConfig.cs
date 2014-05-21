namespace Kafka.Tests.Custom.Server
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using System.Threading;

    using Kafka.Client.Messages;
    using Kafka.Tests.Utils;

    public class TempKafkaConfig : Dictionary<string, string>, IDisposable
    {
         private TempKafkaConfig(Dictionary<string, string> props) : base(props)
         {
             this.ConfigLocation = TestUtils.TempFile();
             File.WriteAllText(this.ConfigLocation, this.SerializeProperties());
         }

         public string ConfigLocation { get; private set; }

         public string LogDir 
         {
            get
            {
                 return this["log.dir"];
            }
         }

        public int MessageMaxBytes
        {
            get
            {
                return this.ContainsKey("message.max.bytes")
                           ? int.Parse(this["message.max.bytes"])
                           : 1000000 + MessageSet.LogOverhead;
            }
        }

        public int BrokerId
        {
            get
            {
                return int.Parse(this["broker.id"]);
            }
        }

        public int Port
        {
            get
            {
                return int.Parse(this["port"]);
            }
        }

        public string SerializeProperties()
        {
            var sb = new StringBuilder();
            foreach (var kvp in this)
            {
                sb.Append(kvp.Key);
                sb.Append("=");
                sb.Append(kvp.Value);
                sb.AppendLine();
            }

            return sb.ToString();
        }

        public static TempKafkaConfig Create(Dictionary<string, string> customProps)
        {
            return new TempKafkaConfig(customProps);
        }

        public void Dispose()
        {
            try
            {
                if (this.ConfigLocation != null)
                {
                    SpinWait.SpinUntil(
                        () =>
                            {
                                File.Delete(this.ConfigLocation);
                                Thread.Sleep(50);
                                return !File.Exists(this.ConfigLocation);
                            },
                        1000);
                }
            }
            catch (Exception e)
            {
            }
            try
            {
                if (this.LogDir != null)
                {
                    Directory.Delete(this.LogDir, true);
                }
            }
            catch
            {
            }
            TestUtils.PortReleased(this.Port);
        }
    }
}