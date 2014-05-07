namespace Kafka.Client.ZKClient
{
    /// <summary>
    ///  Updates the Data of a znode. This is used together with {@link ZkClient#updateDataSerialized(String, DataUpdater)}.
    /// </summary>
    public interface IDataUpdater<TData>
    {
        /// <summary>
        /// Updates the current Data of a znode.
        /// </summary>
        /// <param name="currentData"></param>
        /// <returns></returns>
        TData Update(TData currentData);
    }
}