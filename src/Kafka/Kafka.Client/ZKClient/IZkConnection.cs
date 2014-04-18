namespace Kafka.Client.ZKClient
{
    using System;
    using System.Collections.Generic;

    using Org.Apache.Zookeeper.Data;

    using ZooKeeperNet;

    public interface IZkConnection : IDisposable
    {
         void Connect(IWatcher watcher);

        string Create(string path, byte[] data, CreateMode mode);

        void Delete(string path);

        bool Exists(string path, bool watch);

        List<string> GetChildren(string path, bool watch);

        byte[] ReadData(string path, Stat stat, bool watch);

        void WriteData(string path, byte[] data, int expectedVersion);

        Stat WriteDataReturnStat(string path, byte[] data, int expectedVersion);

        ZooKeeper.States ZookeeperState { get; }

        long GetCreateTime(string path);

        string Servers { get;  }
    }
}