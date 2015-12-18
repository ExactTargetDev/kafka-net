namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    using Kafka.Client.Utils;
    using Kafka.Client.ZKClient;

    using ZooKeeperNet;

    using log4net;

    public class ZookeeperTopicEventWatcher
    {
        private readonly ZkClient zkClient;

        private readonly ITopicEventHandler<string> eventHandler;

        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private object @lock = new object();

        public ZookeeperTopicEventWatcher(ZkClient zkClient, ITopicEventHandler<string> eventHandler)
        {
            this.zkClient = zkClient;
            this.eventHandler = eventHandler;
            this.StartWatchingTopicEvents();
        }

        public void StartWatchingTopicEvents()
        {
            var topicEventListener = new ZkTopicEventListener(this);
            ZkUtils.MakeSurePersistentPathExists(zkClient, ZkUtils.BrokerTopicsPath);

            this.zkClient.SubscribeStateChanges(new ZkSessionExpireListener(this, topicEventListener));

            var topics = this.zkClient.SubscribeChildChanges(ZkUtils.BrokerTopicsPath, topicEventListener);

            // call to bootstrap topic list
            topicEventListener.HandleChildChange(ZkUtils.BrokerTopicsPath, topics);
        }

        public void StopWatchingTopicEvents()
        {
            this.zkClient.UnsubscribeAll();
        }

        public void Shutdown()
        {
            lock (this.@lock)
            {
                Logger.Info("Shutting down topic event watcher.");
                if (this.zkClient != null)
                {
                    this.StopWatchingTopicEvents();
                }
                else
                {
                    Logger.Warn("Cannot shutdown since the embedded zookeeper client has already closed.");
                }
            }
        }

        internal class ZkTopicEventListener : IZkChildListener
        {
            private readonly ZookeeperTopicEventWatcher parent;

            public ZkTopicEventListener(ZookeeperTopicEventWatcher parent)
            {
                this.parent = parent;
            }

            public void HandleChildChange(string parentPath, IEnumerable<string> currentChilds)
            {
                lock (this.parent.@lock)
                {
                    try
                    {
                        if (this.parent.zkClient != null)
                        {
                            var latestTopics = this.parent.zkClient.GetChildren(ZkUtils.BrokerTopicsPath);
                            Logger.DebugFormat("all topics: {0}", string.Join(", ", latestTopics));
                            this.parent.eventHandler.HandleTopicEvent(latestTopics);
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.Error("Error in handling child changes", e);
                    }
                }
            }
        }

        internal class ZkSessionExpireListener : IZkStateListener
        {
            private readonly ZookeeperTopicEventWatcher parent;

            private readonly ZkTopicEventListener topicEventListener;

            public ZkSessionExpireListener(ZookeeperTopicEventWatcher parent, ZkTopicEventListener topicEventListener)
            {
                this.parent = parent;
                this.topicEventListener = topicEventListener;
            }

            public void HandleStateChanged(KeeperState state)
            {
            }

            public void HandleNewSession()
            {
                lock (this.parent.@lock)
                {
                    if (this.parent.zkClient != null)
                    {
                        Logger.Info("ZK expired: resubscribing topic event listener to topic registry");
                        this.parent.zkClient.SubscribeChildChanges(ZkUtils.BrokerTopicsPath, this.topicEventListener);
                    }
                }
            }
        }
    }
}