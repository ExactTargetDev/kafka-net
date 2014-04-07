namespace Kafka.Client.Client
{
    using System.Collections.Generic;

    using Kafka.Client.Cluster;

    using System.Linq;

    using Kafka.Client.Utils;

    /// <summary>
    ///  Helper functions common to clients (producer, consumer, or admin)
    /// </summary>
    public static class ClientUtils
    {
        //TODO: fetchTopicMetadata
        //TODO: fetchTopicMetadata


        /// <summary>
        /// Parse a list of broker urls in the form host1:port1, host2:port2, ... 
        /// </summary>
        /// <param name="brokerListStr"></param>
        /// <returns></returns>
        public static IList<Broker> ParseBrokerList(string brokerListStr)
        {
             IList<string> brokersStr = Util.ParseCsvList(brokerListStr);
            brokersStr.Select(
                (s, i) =>
                    {
                        var brokerStr = s;
                        var brokerId = i;
                        var brokerInfos = brokerStr.Split(':');
                        var hostName = brokerInfos[0];
                        var port = int.Parse(brokerInfos[1]);
                        return new Broker(brokerId, hostName, port);
                    });
        }  
    }
}