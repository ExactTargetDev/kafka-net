namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.Contracts;
    using System.Linq;
    using System.Reflection;

    using Kafka.Client.Common;
    using Kafka.Client.Extensions;
    using Kafka.Client.Utils;
    using Kafka.Client.ZKClient;

    using log4net;

    using Newtonsoft.Json.Linq;

    public abstract class TopicCount
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public const string WhiteListPattern = "white_list";

        public const string BlackListPattern = "black_list";

        public const string StaticPattern = "static";

        public static TopicCount ConstructTopicCount(string group, string consumerId, ZkClient zkClient)
        {
            var dirs = new ZKGroupDirs(group);
            var topicCountString = ZkUtils.ReadData(zkClient, dirs.ConsumerRegistryDir + "/" + consumerId).Item1;
            string subscriptionPattern;
            IDictionary<string, int> topMap;
            try
            {
                var parsedJson = JObject.Parse(topicCountString);
                if (parsedJson != null)
                {
                    var pattern = parsedJson.Get("pattern");
                    if (pattern != null)
                    {
                        subscriptionPattern = pattern.Value<string>();
                    }
                    else
                    {
                        throw new KafkaException("error constructing TopicCount:" + topicCountString);
                    }

                    var topMapObject = (IEnumerable<KeyValuePair<string, JToken>>)parsedJson.Get("subscription");
                    if (topMapObject != null)
                    {
                        topMap = topMapObject.ToDictionary(x => x.Key, x => x.Value.Value<int>());
                    }
                    else
                    {
                        throw new KafkaException("error constructing TopicCount:" + topicCountString);
                    }
                }
                else
                {
                    throw new KafkaException("error constructing TopicCount:" + topicCountString);
                }
            }
            catch (Exception e)
            {
                Logger.Error("error parsing consumer json string " + topicCountString, e);
                throw;
            }

            var hasWhiteList = WhiteListPattern.Equals(subscriptionPattern);
            var hasBlackList = BlackListPattern.Equals(subscriptionPattern);

            if (topMap.Count == 0 || !(hasWhiteList || hasBlackList))
            {
                return new StaticTopicCount(consumerId, topMap);
            }
            else
            {
                var regex = topMap.First().Key;
                var numStreams = topMap.First().Value;
                TopicFilter filter = hasWhiteList ? (TopicFilter)new Whitelist(regex) : new Blacklist(regex);
                return new WildcardTopicCount(zkClient, consumerId, filter, numStreams);
            }
        }

        public static StaticTopicCount ConstructTopicCount(string consumerIdString, IDictionary<string, int> topicCount)
        {
            return new StaticTopicCount(consumerIdString, topicCount);
        }

        public static WildcardTopicCount ConstructTopicCount(
            string consumerIdString, TopicFilter filter, int numStream, ZkClient zkClient)
        {
            return new WildcardTopicCount(zkClient, consumerIdString, filter, numStream);
        }

        public abstract IDictionary<string, ISet<string>> GetConsumerThreadIdsPerTopic();

        public virtual IDictionary<string, int> TopicCountMap { get; protected set; }

        public abstract string Pattern { get; }

        protected IDictionary<string, ISet<string>> MakeConsumerThreadIdsPerTopic(
            string consumerIdString, IDictionary<string, int> topicCountMap)
        {
            var consumerThreadIdsPerTopicMap = new Dictionary<string, ISet<string>>();

            foreach (var topicAndNConsumers in topicCountMap)
            {
                var topic = topicAndNConsumers.Key;
                var numberConsumers = topicAndNConsumers.Value;
                var consumerSet = new HashSet<string>();
                Contract.Assert(numberConsumers >= 1);
                for (var i = 0; i < numberConsumers; i++)
                {
                    consumerSet.Add(consumerIdString + "-" + i);
                }

                consumerThreadIdsPerTopicMap[topic] = consumerSet;
            }

            return consumerThreadIdsPerTopicMap;
        } 
    }

    public class StaticTopicCount : TopicCount
    {
        public string ConsumerIdString { get; private set; }

        public StaticTopicCount(string consumerIdString, IDictionary<string, int> topicCountMap)
        {
            this.ConsumerIdString = consumerIdString;
            this.TopicCountMap = topicCountMap;
        }

        public override IDictionary<string, ISet<string>> GetConsumerThreadIdsPerTopic()
        {
            return this.MakeConsumerThreadIdsPerTopic(ConsumerIdString, TopicCountMap);
        }

        protected bool Equals(StaticTopicCount other)
        {
            return string.Equals(this.ConsumerIdString, other.ConsumerIdString) && Equals(this.TopicCountMap, other.TopicCountMap);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj.GetType() != this.GetType())
            {
                return false;
            }

            return Equals((StaticTopicCount)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((this.ConsumerIdString != null ? this.ConsumerIdString.GetHashCode() : 0) * 397) ^ (this.TopicCountMap != null ? this.TopicCountMap.GetHashCode() : 0);
            }
        }

        public override string Pattern
        {
            get
            {
                return StaticPattern;
            }
        }
    }

    public class WildcardTopicCount : TopicCount
    {
        public ZkClient ZkClient { get; private set; }

        public string ConsumerIdString { get; private set; }

        public TopicFilter TopicFilter { get; private set; }

        public int NumStreams { get; private set; }

        public WildcardTopicCount(ZkClient zkClient, string consumerIdString, TopicFilter topicFilter, int numStreams)
        {
            this.ZkClient = zkClient;
            this.ConsumerIdString = consumerIdString;
            this.TopicFilter = topicFilter;
            this.NumStreams = numStreams;
        }

        public override IDictionary<string, ISet<string>> GetConsumerThreadIdsPerTopic()
        {
            var wildcardTopics = ZkUtils.GetChildrenParentMayNotExist(this.ZkClient, ZkUtils.BrokerTopicsPath)
                   .Where(x => TopicFilter.IsTopicAllowed(x))
                   .ToList();
            return this.MakeConsumerThreadIdsPerTopic(this.ConsumerIdString, wildcardTopics.ToDictionary(x => x, v => this.NumStreams));
        }

        public override IDictionary<string, int> TopicCountMap 
        {
            get
            {
                return new Dictionary<string, int>
                           {
                             { TopicFilter.Regex, this.NumStreams}
                           };
            }

            protected set
            {
                throw new NotSupportedException();
            }
        }

        public override string Pattern
        {
            get
            {
                if (TopicFilter is Whitelist)
                {
                    return WhiteListPattern;
                } 
                if (TopicFilter is Blacklist)
                {
                    return BlackListPattern;
                }
                throw new InvalidOperationException();
            }
        }
    }
}