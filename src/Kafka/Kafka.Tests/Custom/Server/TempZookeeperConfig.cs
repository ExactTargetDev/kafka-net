namespace Kafka.Tests.Custom.Server
{
    using System;
    using System.IO;
    using System.Text;

    using Kafka.Tests.Utils;

    /// <summary>
    /// Creates zookeeper props files on disk
    /// </summary>
    public class TempZookeeperConfig : IDisposable
    {
        public string DataDir { get; private set; }

        public int Port { get; private set; }

        public string ConfigLocation { get; private set; }

        public TempZookeeperConfig(int port)
        {
            this.Port = port;
            this.DataDir = TestUtils.TempDir().FullName;
            this.ConfigLocation = TestUtils.TempFile();
            File.WriteAllText(this.ConfigLocation, this.SerializeProperties());
        }

        public string SerializeProperties()
        {
            var sb = new StringBuilder();
            sb.Append("dataDir=");
            sb.Append(this.DataDir);
            sb.AppendLine();
            sb.Append("clientPort=");
            sb.Append(this.Port);
            sb.AppendLine();
            sb.Append("maxClientCnxns=0");
            sb.AppendLine();
            return sb.ToString();
        }

        public void Dispose()
        {
            try
            {
                Directory.Delete(this.DataDir, true);
            }
            catch
            {
            }

            try
            {
                File.Delete(this.ConfigLocation);
            }
            catch
            {
            }

            TestUtils.PortReleased(this.Port);
        }
    }
}