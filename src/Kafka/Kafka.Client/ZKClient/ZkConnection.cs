namespace Kafka.Client.ZKClient
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Threading;

    using Kafka.Client.ZKClient.Exceptions;

    using log4net;

    using Org.Apache.Zookeeper.Data;

    using ZooKeeperNet;

    public class ZkConnection : IZkConnection
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        /** It is recommended to use quite large sessions timeouts for ZooKeeper. */
        private static readonly TimeSpan DefaultSessionTimeout = TimeSpan.FromSeconds(30);

        private ZooKeeper _zk;

        private object _zookeeperLock = new object();

        private string _servers;

        private TimeSpan _sessionTimeOut;

        public ZkConnection(string zkServers)
            : this(zkServers, DefaultSessionTimeout)
        {
        }

        public ZkConnection(string zkServers, TimeSpan sessionTimeOut)
        {
            _servers = zkServers;
            _sessionTimeOut = sessionTimeOut;
        }

        public void Connect(IWatcher watcher) {
            Monitor.Enter(_zookeeperLock);
            try 
            {
                if (_zk != null) {
                    throw new Exception("zk client has already been started");
                }

                try {
                    Logger.Debug("Creating new ZookKeeper instance to connect to " + _servers + ".");
                    _zk = new ZooKeeper(_servers, _sessionTimeOut, watcher);
                } catch (Exception e) {
                    throw new ZkException("Unable to connect to " + _servers, e);
                }

            } 
            finally 
            {
                Monitor.Exit(this._zookeeperLock);
            }
        }

        public void Dispose() {
            Monitor.Enter(this._zookeeperLock);
            try {
                if (_zk != null) {
                    Logger.Debug("Closing ZooKeeper connected to " + _servers);
                    _zk.Dispose();
                    _zk = null;
                }
            } finally {
                Monitor.Exit(this._zookeeperLock);
            }
        }

        public string Create(string path, byte[] data, CreateMode mode) 
        {
            return _zk.Create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
        }

        public void Delete(string path) 
        {
            _zk.Delete(path, -1);
        }

        public bool Exists(string path, bool watch) 
        {
            return _zk.Exists(path, watch) != null;
        }

        public List<string> GetChildren(string path, bool watch) 
        {
            return _zk.GetChildren(path, watch);
        }

        public byte[] ReadData(string path, Stat stat, bool watch) 
        {
            return _zk.GetData(path, watch, stat);
        }

        public void WriteData(String path, byte[] data) 
        {
            WriteData(path, data, -1);
        }

        public void WriteData(String path, byte[] data, int version) 
        {
            _zk.SetData(path, data, version);
        }

        public Stat WriteDataReturnStat(String path, byte[] data, int version) 
        {
            return _zk.SetData(path, data, version);
        }

        public ZooKeeper.States ZookeeperState 
        {
            get
            {
                return _zk != null ? _zk.State : null;
            }
        }

        public ZooKeeper Zookeeper 
        {
            get
            {
                return _zk;
            }
        }

        public long GetCreateTime(string path) 
        {
            var stat = _zk.Exists(path, false);
            if (stat != null)
            {
                return stat.Ctime;
            }
            return -1;
        }

        public String Servers 
        {
            get
            {
                return _servers;
            }
            
        }

    }
}