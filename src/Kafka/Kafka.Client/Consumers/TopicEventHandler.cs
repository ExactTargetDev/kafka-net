namespace Kafka.Client.Consumers
{
    using System.Collections.Generic;

    public interface ITopicEventHandler<T>
    {
        void HandleTopicEvent(IEnumerable<T> allTopics);
    }
}