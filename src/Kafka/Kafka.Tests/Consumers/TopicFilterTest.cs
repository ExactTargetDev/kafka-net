namespace Kafka.Tests.Consumers
{
    using Kafka.Client.Consumers;

    using Xunit;

    public class TopicFilterTest
    {
        [Fact]
         public void TestWhitelists()
        {
            var topicFilter1 = new Whitelist("white1,white2");
            Assert.True(topicFilter1.IsTopicAllowed("white2"));
            Assert.False(topicFilter1.IsTopicAllowed("black1"));

            var topicFilter2 = new Whitelist(".+");
            Assert.True(topicFilter2.IsTopicAllowed("alltopics"));

            var topicFilter3 = new Whitelist("white_listed-topic.+");
            Assert.True(topicFilter3.IsTopicAllowed("white_listed-topic1"));
            Assert.False(topicFilter3.IsTopicAllowed("black1"));
        }

        [Fact]
        public void TestBlacklists()
        {
            var topicFilter1 = new Blacklist("black1");
        }
    }
}