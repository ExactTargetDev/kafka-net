namespace Kafka.Tests.Zk
{
    using Kafka.Client.Utils;
    using Kafka.Client.ZKClient;

    using Xunit;

    public class ZKEphemeralTest : ZooKeeperTestHarness
    {
        public const int ZkSessionTimeoutMs = 1000;

        [Fact]
        public void TestEphemeralNodeCleanup()
        {
            var zkClient = new ZkClient(ZkConnect, ZkSessionTimeoutMs, ZkConnectionTimeout, new ZkStringSerializer());

            try
            {
                ZkUtils.CreateEphemeralPathExpectConflict(zkClient, "/tmp/zktest", "node created");
            }
            catch
            {
                // ok
            }

            var testData = ZkUtils.ReadData(this.ZkClient, "/tmp/zktest").Item1;
            Assert.NotNull(testData);
            zkClient.Dispose();
            zkClient = new ZkClient(ZkConnect, ZkSessionTimeoutMs, ZkConnectionTimeout, new ZkStringSerializer());
            var nodeExists = ZkUtils.PathExists(zkClient, "/tmp/zktest");
            Assert.False(nodeExists);
        }
    }
}