namespace Kafka.Tests.Utils
{
    using System.Configuration;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// Utility class usefull to create 
    /// </summary>
    public static class KafkaRunClassHelper
    {
        public const string ZookeeperMainClass = "org.apache.zookeeper.server.quorum.QuorumPeerMain";

        public const string KafkaServerMainClass = "kafka.Kafka";

        public static string KafkaInstallationDir
        {
            get
            {
                return ConfigurationManager.AppSettings["KafkaInstallationDir"];
            }
        }

        public static string JavaExec
        {
            get
            {
                return ConfigurationManager.AppSettings["JavaExec"];
            }
        }

        public static Process Run(string mainClass, string args)
        {
            var startInfo = new ProcessStartInfo();
            var p = new Process();

            startInfo.UseShellExecute = false;
            startInfo.Arguments = MakeArgs(mainClass, args);
            startInfo.FileName = JavaExec;

            p.StartInfo = startInfo;
            p.Start();
            return p;
        }

        private static string MakeArgs(string mainClass, string args)
        {
            var sb = new StringBuilder();

            sb.Append(@" -Xmx512M -Xms512M  -XX:+UseParNewGC -XX:+UseConcMarkSweepGC ");
            sb.AppendFormat(@" -Dlog4j.configuration=file:{0}config\log4j.properties -Dkafka.logs.dir={0}logs ", KafkaInstallationDir);

            var libsDir = Path.Combine(KafkaInstallationDir, "libs");

            var jarsToInclude = Directory.EnumerateFiles(Path.Combine(KafkaInstallationDir, "libs"))
                     .Select(x => Path.Combine(libsDir, x))
                     .ToList();

            var classpath = " -cp " + string.Join(";", jarsToInclude);
            sb.Append(" " + classpath + " ");
            sb.Append(" ");
            sb.Append(mainClass);
            sb.Append(" ");
            sb.Append(args);

            return sb.ToString();
        }
    }
}