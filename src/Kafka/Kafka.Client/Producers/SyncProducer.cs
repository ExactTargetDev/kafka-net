namespace Kafka.Client.Producers
{
    using System;

    using Kafka.Client.Api;
    using Kafka.Client.Cfg;

    internal class SyncProducer : IDisposable
    {
        public SyncProducerConfiguration Config { get; private set; }

        public SyncProducer(SyncProducerConfiguration config)
        {
            this.Config = config;
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public ProducerResponse Send(ProducerRequest producerRequest)
        {
            throw new NotImplementedException();
        }

        //TODO: 
    }
}