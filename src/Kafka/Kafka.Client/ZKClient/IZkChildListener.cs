namespace Kafka.Client.ZKClient
{
    using System.Collections.Generic;

    /// <summary>
    /// An {@link IZkChildListener} can be registered at a {@link ZkClient} for listening on zk child changes for a given
    /// path.
    /// 
    /// Node: Also this listener re-subscribes it watch for the path on each zk event (zk watches are one-timers) is is not
    /// guaranteed that events on the path are missing (see http://zookeeper.wiki.sourceforge.net/ZooKeeperWatches). An
    /// implementation of this class should take that into account.
    /// </summary>
    public interface IZkChildListener
    {
        /// <summary>
        /// Called when the children of the given path changed.
        /// </summary>
        /// <param name="parentPath"></param>
        /// <param name="currentChilds"></param>
        void HandleChildChange(string parentPath, IList<string> currentChilds);
    }
}