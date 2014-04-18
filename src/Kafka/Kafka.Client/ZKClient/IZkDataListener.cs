namespace Kafka.Client.ZKClient
{
    /// <summary>
    /// 
    /// An {@link IZkDataListener} can be registered at a {@link ZkClient} for listening on zk data changes for a given path.
    /// 
    /// Node: Also this listener re-subscribes it watch for the path on each zk event (zk watches are one-timers) is is not
    /// guaranteed that events on the path are missing (see http://zookeeper.wiki.sourceforge.net/ZooKeeperWatches). An
    /// implementation of this class should take that into account.
    /// </summary>
    public interface IZkDataListener
    {
        void HandleDataChange(string dataPath, object data);

        void HandleDataDeleted(string dataPath);
    }
}