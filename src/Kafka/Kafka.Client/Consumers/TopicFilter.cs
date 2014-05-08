namespace Kafka.Client.Consumers
{
    using System;
    using System.Reflection;
    using System.Text.RegularExpressions;

    using log4net;

    public abstract class TopicFilter
    {
        public string RawRegexp { get; private set; }

        public string Regex
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        protected TopicFilter(string rawRegexp)
        {
            this.RawRegexp = rawRegexp;
            //TODO: pattern compile
        }

        public override string ToString()
        {
            return string.Format("Regex: {0}", this.Regex);
        }

        protected static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public abstract bool IsTopicAllowed(string topic);
    }

    public class Whitelist : TopicFilter
    {
        public Whitelist(string rawRegexp)
            : base(rawRegexp)
        {
        }

        public override bool IsTopicAllowed(string topic)
        {
            var allowed = new Regex(RawRegexp).IsMatch(topic);

            Logger.DebugFormat("{0} {1}", topic, allowed ? "allowed" : "filtered");

            return allowed;
        }
    }

    public class Blacklist : TopicFilter
    {
        public Blacklist(string rawRegexp)
            : base(rawRegexp)
        {
        }

        public override bool IsTopicAllowed(string topic)
        {
            var allowed = !new Regex(RawRegexp).IsMatch(topic);

            Logger.DebugFormat("{0} {1}", topic, allowed ? "allowed" : "filtered");

            return allowed;
        }
    }
}