# .NET Kafka Client

This is a .NET implementation of a client for Kafka using C#.  It provides for an implementation that covers most basic functionalities to include a simple Producer and Consumer.

## Producer

The Producer can send one or more messages to Kafka in both a synchronous and asynchronous fashion.

### Producer Usage

    var producerConfig1 = new ProducerConfig();
    producerConfig1.ProducerType = ProducerTypes.Async; // (or sync)
    // ...
    // other configuration settings (described below)
    // ...
    producerConfig1.Brokers = new List<BrokerConfiguration>
               {
                  new BrokerConfiguration
                      {
                         BrokerId = 0, Host = "localhost", Port = 9092
                      }
              };
    producerConfig1.KeySerializer = typeof(StringEncoder).AssemblyQualifiedName;
    producerConfig1.Serializer = typeof(StringEncoder).AssemblyQualifiedName;
    using (var producer1 = new Producer<string, string>(producerConfig1))
        producer1.Send(new KeyedMessage<string, string>(topic, "test", "test1"));
    }
    


## Consumer

The consumer has several functions of interest: `CommitOffsets` and `CreateMessageStreams`.  `CommitOffsets` will commit offset that was already consumed by client. You can also configure to autocommit offsets. `CreateMessageStreams` creates streams from which we can read messages. 

### Consumer Usage

    var consumerConfig1 = new ConsumerConfig("localhost", "2181", "group1");
    var zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1);
    var topicMessageStreams1 =
        zkConsumerConnector1.CreateMessageStreams(
            new Dictionary<string, int> { { Topic, 1 } }, new StringDecoder(), new StringDecoder());

    foreach (var messageAndMetadata in topicMessageStreams1[Topic][0])
    {
        Console.WriteLine(messageAndMetadata.Message);
    }

