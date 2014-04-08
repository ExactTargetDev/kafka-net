namespace Kafka.Client.Cluster
{
    using System;
    using System.Globalization;
    using System.IO;

    using Kafka.Client.Api;
    using Kafka.Client.Common;
    using Kafka.Client.Extensions;
    using Kafka.Client.Serializers;

    public class Broker
    {
        public int Id { get; private set; }

        public string Host { get; private set; }

        public int Port { get; private set; }

        public Broker(int id, string host, int port)
        {
            this.Id = id;
            this.Host = host;
            this.Port = port;
        }

        public override string ToString()
        {
            return string.Format("Id: {0}, Host: {1}, Port: {2}", this.Id, this.Host, this.Port);
        }

        public string GetConnectionString()
        {
            return this.Host + ":" + this.Port;
        }

        public void WriteTo(MemoryStream buffer)
        {
            buffer.PutInt(this.Id);
            ApiUtils.WriteShortString(buffer, this.Host);
            buffer.PutInt(this.Port);
        }

        public int SizeInBytes { 
            get
            {
                return ApiUtils.ShortStringLength(this.Host); ///* host name */ + 4 /* port */ + 4 /* broker id*/
            }
        }


        /* TODO: not necessary ?
        public static Broker CreateBroker(int id, string brokerInfoString)
        {
            //TODO: check this method!
            if (string.IsNullOrEmpty(brokerInfoString))
            {
                throw new BrokerNotAvailableException(string.Format("Broker id {0} does not exist", id));
            }
            var brokerInfo = brokerInfoString.Split(':');
            if (brokerInfo.Length > 2)
            {
                int port;
                if (int.TryParse(brokerInfo[2], NumberStyles.Integer, CultureInfo.InvariantCulture, out port))
                {
                    return new Broker(id, brokerInfo[0], brokerInfo[1]);
                }
                else
                {
                    throw new ArgumentException(string.Format(CultureInfo.CurrentCulture, "{0} is not a valid integer", brokerInfo[2]));
                }
            }
            else
            {
                throw new ArgumentException(string.Format(CultureInfo.CurrentCulture, "{0} is not a valid BrokerInfoString", brokerInfoString));
            }
        }*/

        internal static Broker ReadFrom(MemoryStream buffer)
        {
            var id = buffer.GetInt();
            var host = ApiUtils.ReadShortString(buffer);
            var port = buffer.GetInt();
            return new Broker(id, host, port);
        }

    }
}