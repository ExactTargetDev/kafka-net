namespace Kafka.Client.ZKClient
{
    using ZooKeeperNet;

    public interface IZkStateListener
    {
        /// <summary>
        /// Called when the zookeeper connection state has changed.
        /// </summary>
        /// <param name="state"></param>
        void HandleStateChanged(KeeperState state);

        /// <summary>
        /// Called after the zookeeper session has expired and a new session has been created. You would have to re-create
        /// any ephemeral nodes here.
        /// </summary>
        void HandleNewSession();
    }
}