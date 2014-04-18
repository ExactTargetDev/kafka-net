namespace Kafka.Client.Consumers
{
    using System.Text.RegularExpressions;

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
}