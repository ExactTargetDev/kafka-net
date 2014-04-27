namespace Kafka.Client.Api
{
    public static class Request
    {
        public const int OrdinaryConsumerId = -1;

        public const int DebuggingConsumerId = -2;

        /// <summary>
        /// Followers use broker id as the replica id, which are non-negative int.
        /// </summary>
        /// <param name="replicaId"></param>
        /// <returns></returns>
        public static bool IsReplicaIdFromFollower(int replicaId)
        {
            return replicaId >= 0;
        }
    }
}