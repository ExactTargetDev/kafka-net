namespace Kafka.Client.ZKClient.Exceptions
{
    using System;

    using ZooKeeperNet;

    public class ZkException : Exception
    {
        public ZkException()
        {
        }

        public ZkException(string message)
            : base(message)
        {
        }

        public ZkException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public static ZkException Create(KeeperException e)
        {
            switch (e.GetCode())
            {
                case KeeperException.Code.NONODE:
                    return new ZkNoNodeException(e.Message, e);
                case KeeperException.Code.BADVERSION:
                    return new ZkBadVersionException(e.Message, e);
                case KeeperException.Code.NODEEXISTS:
                    return new ZkNodeExistsException(e.Message, e);
                default:
                    return new ZkException(e.Message, e);
            }
        }
    }
}