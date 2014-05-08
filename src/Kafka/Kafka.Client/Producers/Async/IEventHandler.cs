namespace Kafka.Client.Producers.Async
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Handler that dispatches the batched Data from the queue.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public interface IEventHandler<TKey, TValue> : IDisposable
    {
        /// <summary>
        /// Callback to dispatch the batched Data and send it to a Kafka server
        /// </summary>
        /// <param name="events"></param>
        void Handle(IEnumerable<KeyedMessage<TKey, TValue>> events);
    }
}