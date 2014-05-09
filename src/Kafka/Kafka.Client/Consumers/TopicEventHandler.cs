namespace Kafka.Client.Consumers
{
    using System.Collections.Generic;

    public interface ITopicEventHandler<T>
    {
        void HandleTopicEvent(List<T> allTopics);
    }
}