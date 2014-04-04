namespace Kafka.Client.Producers.Async
{
    using System;

    /// <summary>
    /// Handler that dispatches the batched data from the queue.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public interface IEventHandler<TKey, TValue> : IDisposable
    {
        /// <summary>
        /// Callback to dispatch the batched data and send it to a Kafka server
        /// </summary>
        /// <param name="events"></param>
        void Handle(KeyedMessage<TKey, TValue>[] events);

    }
}