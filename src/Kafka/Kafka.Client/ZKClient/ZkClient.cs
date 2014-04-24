namespace Kafka.Client.ZKClient
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Reflection;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    using Kafka.Client.Utils;
    using Kafka.Client.ZKClient.Exceptions;
    using Kafka.Client.ZKClient.Serialize;

    using Org.Apache.Zookeeper.Data;

    using ZooKeeperNet;

    using log4net;

    using Kafka.Client.Extensions;

    public class ZkClient : IWatcher, IDisposable
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        protected IZkConnection _connection;

        private readonly IDictionary<string, ConcurrentHashSet<IZkChildListener>> _childListener = new ConcurrentDictionary<string, ConcurrentHashSet<IZkChildListener>>();

        private readonly ConcurrentDictionary<String, ConcurrentHashSet<IZkDataListener>> _dataListener = new ConcurrentDictionary<string, ConcurrentHashSet<IZkDataListener>>();

        //TODO: use concurrent version!!
        private readonly ConcurrentHashSet<IZkStateListener> _stateListener = new ConcurrentHashSet<IZkStateListener>();

        private KeeperState _currentState;

        public KeeperState CurrentState { 
            get
            {
                return _currentState;
            }
            
            set
            {
                this.EventLock.Lock();
                try
                {
                    this._currentState = value;
                }
                finally
                {
                    this.EventLock.Unlock();
                }
            }
        }

        public ZkLock EventLock { get; private set; }

        public bool ShutdownTrigger { get; set; }

        private ZkEventThread _eventThread;

        private Thread _zookeeperEventThread;

        public IZkSerializer ZkSerializer { get; set; }

        public ZkClient(string serverstring)
            : this(serverstring, int.MaxValue)
        {
        }

        public ZkClient(string zkServers, int connectionTimeout)
            : this(new ZkConnection(zkServers), connectionTimeout)
        {
        }

        public ZkClient(string zkServers, int sessionTimeout, int connectionTimeout) : this(new ZkConnection(zkServers, TimeSpan.FromMilliseconds(sessionTimeout)), connectionTimeout)
        {
        }

        public ZkClient(string zkServers, int sessionTimeout, int connectionTimeout, IZkSerializer zkSerializer)
            : this(new ZkConnection(zkServers, TimeSpan.FromMilliseconds(sessionTimeout)), connectionTimeout, zkSerializer)
        {
        }

        public ZkClient(IZkConnection connection)
            : this(connection, int.MaxValue)
        {
        }

        public ZkClient(IZkConnection connection, int connectionTimeout) : this(connection, connectionTimeout, new SerializableSerializer())
        {
        }

        public ZkClient(IZkConnection zkConnection, int connectionTimeout, IZkSerializer zkSerializer)
        {
            this._connection = zkConnection;
            this.ZkSerializer = zkSerializer;
            this.EventLock = new ZkLock();
            this.Connect(connectionTimeout, this);
        }

        public List<string> SubscribeChildChanges(string path, IZkChildListener listener) {
            lock (_childListener)
            {
                ConcurrentHashSet<IZkChildListener> listeners = _childListener.Get(path);

                if (listeners == null)
                {
                    listeners = new ConcurrentHashSet<IZkChildListener>();
                    _childListener[path] = listeners;
                }

                listeners.Add(listener);
            }
            return WatchForChilds(path);
        }

        public void UnsubscribeChildChanges(string path, IZkChildListener childListener) {
            lock (_childListener)
            {
                ConcurrentHashSet<IZkChildListener> listeners = _childListener.Get(path);
                if (listeners != null)
                {
                    listeners.TryRemove(childListener);
                }
            }
        }

         public void SubscribeDataChanges(string path, IZkDataListener listener) {
            ConcurrentHashSet<IZkDataListener> listeners;
            lock (_dataListener)
            {
                listeners = _dataListener.Get(path);
                if (listeners == null)
                {
                    listeners = new ConcurrentHashSet<IZkDataListener>();
                    _dataListener[path] = listeners;
                }

                listeners.Add(listener);
            }
            WatchForData(path);
            Logger.Debug("Subscribed data changes for " + path);
        }

        public void UnsubscribeDataChanges(string path, IZkDataListener dataListener) {
            lock (_dataListener)
            {

                ConcurrentHashSet<IZkDataListener> listeners = _dataListener.Get(path);
                if (listeners != null)
                {
                    listeners.TryRemove(dataListener);
                }

                if (listeners == null || listeners.Count == 0 )
                {
                    ConcurrentHashSet<IZkDataListener> _;
                    _dataListener.TryRemove(path, out _);
                }
            }
        }

         public void SubscribeStateChanges(IZkStateListener listener) {
            lock (_stateListener) {
                _stateListener.Add(listener);
            }
        }

        public void UnsubscribeStateChanges(IZkStateListener stateListener) {
            lock (_stateListener) {
                _stateListener.TryRemove(stateListener);
            }
        }

        public void UnsubscribeAll() {
            lock (_childListener) {
                _childListener.Clear();
            }
            lock (_dataListener) {
                _dataListener.Clear();
            }
            lock (_stateListener)
            {
                _stateListener.Clear();
            }
        }

        /// <summary>
        /// Create a persistent node.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="createParents"></param>
        public void CreatePersistent(String path, bool createParents = false)
        {
            try 
            {
                Create(path, null, CreateMode.Persistent);
            } 
            catch (ZkNodeExistsException e) 
            {
                if (!createParents) 
                {
                    throw;
                }
            } 
            catch (ZkNoNodeException e) 
            {
                if (!createParents)
                {
                    throw;
                }
                var parentDir = path.Substring(0, path.LastIndexOf('/'));
                CreatePersistent(parentDir, createParents);
                CreatePersistent(path, createParents);
            }
        }

        /// <summary>
        /// Create a persistent node.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="data"></param>
        public void CreatePersistent(string path, object data)
        {
            this.Create(path, data, CreateMode.Persistent);
        }

        /// <summary>
        /// Create a persistent, sequental node.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        public string CreatePersistentSequential(string path, object data)
        {
            return this.Create(path, data, CreateMode.PersistentSequential);
        }

        /// <summary>
        /// Create an ephemeral node.
        /// </summary>
        /// <param name="path"></param>
        public void CreateEphemeral(string path)
        {
            this.Create(path, null, CreateMode.Ephemeral);
        }

        /// <summary>
        /// Create a node.
        /// </summary>
        /// <param name="String"></param>
        /// <returns></returns>
        public String Create(string path, object data, CreateMode mode) 
        {
            if (path == null) 
            {
                throw new NullReferenceException("path must not be null.");
            }

            var bytes = data == null ? null : this.Serialize(data);
            return RetryUntilConnected(() => _connection.Create(path, bytes, mode));
        }

        /// <summary>
        /// Create an ephemeral node.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="data"></param>
        public void CreateEphemeral(string path, object data) 
        {
            this.Create(path, data, CreateMode.Ephemeral);
        }

        /// <summary>
        /// Create an ephemeral, sequential node.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        public string CreateEphemeralSequential(string path, object data) 
        {
            return this.Create(path, data, CreateMode.EphemeralSequential);
        }

        public void Process(WatchedEvent watchedEvent) {
            Logger.Debug("Received event: " + watchedEvent);
            _zookeeperEventThread = Thread.CurrentThread;

            var stateChanged = watchedEvent.Path == null;
            var znodeChanged = watchedEvent.Path != null;
            var dataChanged = watchedEvent.Type == EventType.NodeDataChanged || watchedEvent.Type == EventType.NodeDeleted || watchedEvent.Type == EventType.NodeCreated
                    || watchedEvent.Type == EventType.NodeChildrenChanged;

            this.EventLock.Lock();
            try 
            {
                // We might have to install child change event listener if a new node was created
                if (this.ShutdownTrigger) 
                {
                    Logger.Debug("ignoring event '{" + watchedEvent.Type + " | " + watchedEvent.Path + "}' since shutdown triggered");
                    return;
                }

                if (stateChanged) 
                {
                    this.ProcessStateChanged(watchedEvent);
                }

                if (dataChanged) 
                {
                    this.ProcessDataOrChildChange(watchedEvent);
                }
            } 
            finally 
            {
                if (stateChanged) 
                {
                    this.EventLock.StateChangedCondition.SignalAll();

                    // If the session expired we have to signal all conditions, because watches might have been removed and
                    // there is no guarantee that those
                    // conditions will be signaled at all after an Expired event
                    // TODO PVo write a test for this
                    if (watchedEvent.State == KeeperState.Expired) 
                    {
                        this.EventLock.ZNodeEventCondition.SignalAll();
                        this.EventLock.DataChangedCondition.SignalAll();
                        // We also have to notify all listeners that something might have changed
                        this.FireAllEvents();
                    }
                }

                if (znodeChanged) 
                {
                    this.EventLock.ZNodeEventCondition.SignalAll();
                }

                if (dataChanged) 
                {
                    this.EventLock.DataChangedCondition.SignalAll();
                }

                this.EventLock.Unlock();
                Logger.Debug("Leaving process event");
            }
        }

        private void FireAllEvents() 
        {
            foreach (var entry in _childListener)
            {
                this.FireChildChangedEvents(entry.Key, entry.Value);
            }
            foreach (var entry in _dataListener)
            {
                this.FireDataChangedEvents(entry.Key, entry.Value);
            }
        }

        public List<string> GetChildren(string path) 
        {
            return this.GetChildren(path, this.HasListeners(path));
        }

        protected List<string> GetChildren(string path, bool watch)
        {
            return RetryUntilConnected(() => _connection.GetChildren(path, watch));
        }

        /// <summary>
        /// Counts number of children for the given path.
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public int CountChildren(string path) 
        {
            try 
            {
                return GetChildren(path).Count;
            } 
            catch (ZkNoNodeException e) 
            {
                return 0;
            }
        }

        protected bool Exists(string path, bool watch)
        {
            return RetryUntilConnected(() => _connection.Exists(path, watch));
        }

        public bool Exists(string path) 
        {
            return Exists(path, this.HasListeners(path));
        }

        private void ProcessStateChanged(WatchedEvent watchedEvent) 
        {
            Logger.InfoFormat("zookeeper state changed ({0})", watchedEvent.State);
            CurrentState = watchedEvent.State;
            if (ShutdownTrigger) 
            {
                return;
            }

            try 
            {
                this.FireStateChangedEvent(watchedEvent.State);

                if (watchedEvent.State == KeeperState.Expired)
                {
                    Reconnect();
                    FireNewSessionEvents();
                }

            } 
            catch (Exception e)
            {
                throw new Exception("Exception while restarting zk client", e);
            }
        }

        private void FireNewSessionEvents() 
        {
            foreach (var stateListener in this._stateListener)
            {
                _eventThread.Send(
                    new ZkEvent("New session event sent to " + stateListener)
                        {
                            RunAction =
                                () =>
                                stateListener.HandleNewSession()
                        });
            }
        }

        private void FireStateChangedEvent(KeeperState state)
        {
            foreach (var stateListener in this._stateListener)
            {
                _eventThread.Send(new ZkEvent("State changed to " + state + " sent to " + stateListener) 
                {
                    RunAction = () => stateListener.HandleStateChanged(state)
                });
            }
        }

        private bool HasListeners(string path)
        {
            var dataListeners = _dataListener.Get(path);
            if (dataListeners != null && dataListeners.Count > 0) 
            {
                return true;
            }

            var childListeners = _childListener.Get(path);
            if (childListeners != null && childListeners.Count > 0) 
            {
                return true;
            }
            return false;
        }

        public bool DeleteRecursive(String path) 
        {
            List<String> children;
            try 
            {
                children = GetChildren(path, false);
            } 
            catch (ZkNoNodeException e) 
            {
                return true;
            }

            foreach (var subPath in children) 
            {
                if (!DeleteRecursive(path + "/" + subPath))
                {
                    return false;
                }
            }

            return Delete(path);
        }

        private void ProcessDataOrChildChange(WatchedEvent watchedEvent)
        {
            var path = watchedEvent.Path;

            if (watchedEvent.Type == EventType.NodeChildrenChanged || watchedEvent.Type == EventType.NodeCreated || watchedEvent.Type == EventType.NodeDeleted)
            {
                var childListeners = _childListener.Get(path);
                if (childListeners != null && childListeners.Count > 0)
                {
                    this.FireChildChangedEvents(path, childListeners);
                }
            }

            if (watchedEvent.Type == EventType.NodeDataChanged || watchedEvent.Type == EventType.NodeDeleted || watchedEvent.Type == EventType.NodeCreated)
            {
                var listeners = _dataListener.Get(path);
                if (listeners != null && listeners.Count > 0) {
                    this.FireDataChangedEvents(watchedEvent.Path, listeners);
                }
            }
        }


         private void FireDataChangedEvents(string path, IEnumerable<IZkDataListener> listeners) {
            foreach (var listener in listeners) {
                _eventThread.Send(new ZkEvent("Data of " + path + " changed sent to " + listener) {
                    RunAction = () =>
                        {
                             // reinstall watch
                            this.Exists(path, true);
                            try 
                            {
                                Object data = this.ReadData<object>(path, null, true);
                                listener.HandleDataChange(path, data);
                            } 
                            catch (ZkNoNodeException e) 
                            {
                                listener.HandleDataDeleted(path);
                            }
                        }
                });
            }
        }

         private void FireChildChangedEvents(string path, ConcurrentHashSet<IZkChildListener> childListeners)
         {
            try {
                // reinstall the watch
                foreach (var listener in childListeners) {
                    _eventThread.Send(new ZkEvent("Children of " + path + " changed sent to " + listener) {
                        RunAction = () =>
                            {
                                try {
                                    // if the node doesn't exist we should listen for the root node to reappear
                                    Exists(path);
                                    var children = this.GetChildren(path);
                                    listener.HandleChildChange(path, children);
                                } catch (ZkNoNodeException e) {
                                    listener.HandleChildChange(path, null);
                                }
                            }
                    });
                }
            } 
            catch (Exception e) 
            {
                Logger.Error("Failed to fire child changed event. Unable to getChildren.  ", e);
            }
        }

        public bool WaitUntilExists(String path, TimeSpan time)
        {
            DateTime timeout = DateTime.Now + time;
            Logger.Debug("Waiting until znode '" + path + "' becomes available.");
            if (Exists(path)) {
                return true;
            }
            AcquireEventLock();
            try 
            {
                while (!Exists(path, true))
                {

                    var gotSignal = EventLock.ZNodeEventCondition.Await(time);
                    if (!gotSignal)
                    {
                        return false;
                    }
                }
                return true;
            } 
            catch (ThreadInterruptedException e) 
            {
                throw new ZkInterruptedException("Thread interrupted", e);
            } 
            finally 
            {
                EventLock.Unlock();
            }
        }

        protected ConcurrentHashSet<IZkDataListener> GetDataListener(string path)
        {
            return _dataListener.Get(path);
        }


        public void ShowFolders(Stream output) 
        {
            try
            {
                var bytes = Encoding.UTF8.GetBytes(ZkPathUtil.ToString(this));
                output.Write(bytes, 0, bytes.Length);
            } 
            catch (IOException e) 
            {
                Logger.ErrorFormat("IOException during show folders", e);
            }
        }

        public void WaitUntilConnected()
        {
            this.WaitUntilConnected(TimeSpan.MaxValue);
        }

        public bool WaitUntilConnected(TimeSpan timeout)
        {
            return WaitForKeeperState(KeeperState.SyncConnected, timeout);
        }

        public bool WaitForKeeperState(KeeperState keeperState, TimeSpan timeout) 
        {
            if (_zookeeperEventThread != null && Thread.CurrentThread == _zookeeperEventThread)
            {
                throw new Exception("Must not be done in the zookeeper event thread.");
            }
            Logger.Debug("Waiting for keeper state " + keeperState);
            this.AcquireEventLock();
            try 
            {
                bool stillWaiting = true;
                while (CurrentState != keeperState) 
                {
                    if (!stillWaiting) 
                    {
                        return false;
                    }
                    stillWaiting = EventLock.StateChangedCondition.Await(timeout);
                }
                Logger.Debug("State is " + CurrentState);
                return true;
            } 
            catch (ThreadInterruptedException e) 
            {
                throw new ZkInterruptedException(e);
            } 
            finally 
            {
                EventLock.Unlock();
            }
        }

        private void AcquireEventLock() 
        {
            try
            {
                EventLock.LockInterruptibly();
            } 
            catch (ThreadInterruptedException e) 
            {
                throw new ZkInterruptedException(e);
            }
        }

        public TResult RetryUntilConnected<TResult>(Func<TResult> callable) 
        {
            if (_zookeeperEventThread != null && Thread.CurrentThread == _zookeeperEventThread) 
            {
                throw new Exception("Must not be done in the zookeeper event thread.");
            }
            while (true) 
            {
                try
                {
                    return callable();
                } 
                catch (KeeperException.ConnectionLossException e) 
                {
                    // we give the event thread some time to update the status to 'Disconnected'
                    Thread.Yield();
                    WaitUntilConnected();
                } 
                catch (KeeperException.SessionExpiredException e) 
                {
                    // we give the event thread some time to update the status to 'Expired'
                    Thread.Yield();
                    WaitUntilConnected();
                } 
                catch (KeeperException e) 
                {
                    throw ZkException.Create(e);
                } 
                catch (ThreadInterruptedException e) 
                {
                    throw new ZkInterruptedException(e);
                } 
                catch (Exception e)
                {
                    throw e;
                }
            }
        }


        public bool Delete(string path) 
        {
            try
            {
                RetryUntilConnected<object>(() => { _connection.Delete(path); return null; });
                return true;
            } 
            catch (ZkNoNodeException e) 
            {
                return false;
            }
        }

        private byte[] Serialize(Object data)
        {
            return ZkSerializer.Serialize(data);
        }

        private TResult Derializable<TResult>(byte[] data) 
        {
            if (data == null) 
            {
                return default(TResult);
            }
            return (TResult) ZkSerializer.Deserialize(data);
        }

        public TResult ReadData<TResult>(string path, bool returnNullIfPathNotExists = false)
        {
            TResult data = default(TResult);
            try 
            {
                data = ReadData<TResult>(path, null);
            } 
            catch (ZkNoNodeException e)
            {
                if (!returnNullIfPathNotExists) 
                {
                    throw;
                }
            }
            return data;
        }

        public TResult ReadData<TResult>(string path, Stat stat) {
            return ReadData<TResult>(path, stat, HasListeners(path));
        }

        protected TResult ReadData<TResult>(string path, Stat stat, bool watch)
        {
            var data = RetryUntilConnected(
                () => _connection.ReadData(path, stat, watch)
            );
            return Derializable<TResult>(data);
        }

        public void WriteData(string path, object obj) 
        {
            this.WriteData(path, obj, -1);
        }


         public void UpdateDataSerialized<TResult>(string path, IDataUpdater<TResult> updater) 
         {
            var stat = new Stat();
            bool retry;
            do 
            {
                retry = false;
                try 
                {
                    var oldData = ReadData<TResult>(path, stat);
                    var newData = updater.Update(oldData);
                    WriteData(path, newData, stat.Version);
                } 
                catch (ZkBadVersionException e) 
                {
                    retry = true;
                }
            }
            while (retry);
        }

        public void WriteData(string path, object datat, int expectedVersion) 
        {
    	    this.WriteDataReturnStat(path, datat, expectedVersion);
        }

        public Stat WriteDataReturnStat(string path, object datat, int expectedVersion) 
        {
            var data = this.Serialize(datat);
            return RetryUntilConnected(
                () => _connection.WriteDataReturnStat(path, data, expectedVersion)
            );
        }

        public void WatchForData(string path) 
        {
            RetryUntilConnected<object>(
                () =>
                    { 
                        _connection.Exists(path, true);
                        return null;
                    }
            );
        }

        /// <summary>
        /// Installs a child watch for the given path.
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public List<String> WatchForChilds(string path) 
        {
            if (_zookeeperEventThread != null && Thread.CurrentThread == _zookeeperEventThread) {
                throw new Exception("Must not be done in the zookeeper event thread.");
            }
            return RetryUntilConnected(
                () =>
                    { 
                        Exists(path, true);
                        try
                        {
                            return GetChildren(path, true);
                        }
                        catch (ZkNoNodeException e)
                        {
                            // ignore, the "exists" watch will listen for the parent node to appear
                        }
                        return null;
                    });
        }

        public void Connect(long maxMsToWaitUntilConnected, IWatcher watcher) {
            bool started = false;
            try
            {
                EventLock.LockInterruptibly();
                ShutdownTrigger = false;
                _eventThread = new ZkEventThread(_connection.Servers);
                _eventThread.Start();
                _connection.Connect(watcher);

                Logger.Debug("Awaiting connection to Zookeeper server");
                if (!WaitUntilConnected(TimeSpan.FromMilliseconds(maxMsToWaitUntilConnected))) 
                {
                    throw new ZkTimeoutException("Unable to connect to zookeeper server within timeout: " + maxMsToWaitUntilConnected);
                }
                started = true;
            }
            catch (ThreadInterruptedException e) 
            {
                ZooKeeper.States state = _connection.ZookeeperState;
                throw new Exception("Not connected with zookeeper server yet. Current state is " + state);
            } 
            finally 
            {
                EventLock.Unlock();

                // we should close the zookeeper instance, otherwise it would keep
                // on trying to connect
                if (!started) 
                {
                    this.Dispose();
                }
            }
        }

        public long GetCreationTime(String path) {
            try
            {
                EventLock.LockInterruptibly();
                return _connection.GetCreateTime(path);
            } 
            catch (KeeperException e)
            {
                throw ZkException.Create(e);
            } 
            catch (ThreadInterruptedException e) 
            {
                throw new ZkInterruptedException(e);
            } finally {
                EventLock.Unlock();
            }
        }


         public void Dispose()
         {
            if (_connection == null) {
                return;
            }
            Logger.Debug("Closing ZkClient...");
             EventLock.Lock();
            try
            {
                ShutdownTrigger = true;
                _eventThread.Interrupt();
                _eventThread.Join(2000);
                _connection.Dispose();
                _connection = null;
            } 
            catch (ThreadInterruptedException e) 
            {
                throw new ZkInterruptedException(e);
            } 
            finally 
            {
                EventLock.Unlock();
            }
            Logger.Debug("Closing ZkClient...done");
        }

        private void Reconnect()
        {
            EventLock.Lock();
            try 
            {
                _connection.Dispose(); //TOOD: connect?  / close?
                _connection.Connect(this);
            } 
            catch (ThreadInterruptedException e)
            {
                throw new ZkInterruptedException(e);
            } 
            finally 
            {
                EventLock.Unlock();
            }
        }

        public int NumberOfListeners 
        {
            get
            {
                var listeners = 0;
                foreach (var childListeners in _childListener.Values)
                {
                    listeners += childListeners.Count;
                }
                foreach (var dataListeners in _dataListener.Values)
                {
                    listeners += dataListeners.Count;
                }
                listeners += _stateListener.Count;

                return listeners;
            }
        }

    }
}