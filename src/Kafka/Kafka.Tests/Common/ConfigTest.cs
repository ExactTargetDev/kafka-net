namespace Kafka.Tests.Common
{
    using System.Collections.Generic;
    using System.Linq;

    using Kafka.Client.Consumers;
    using Kafka.Client.Producers;

    using Xunit;

    public class ConfigTest
    {
         [Fact]
         public void TestInvalidClientIds()
         {
             var badChars = new List<string> { "/", "\\", ",", "\0", ":", "\"", "\'", ";", "*", "?", " ", "\t", "\r", "\n", "=" };
             var invalidClientIds = badChars.Select(weirdChar => "Is " + weirdChar + "illegal").ToList();

             foreach (string invalidClientId in invalidClientIds)
             {
                 try
                 {
                     ProducerConfig.ValidateClientId(invalidClientId);
                     Assert.False(true);
                 }
                 catch
                 {
                 }
             }

            var validClientIds = new List<string> { "valid", "CLIENT", "iDs", "ar6", "VaL1d", "_0-9_.", string.Empty };

            foreach (string validClientId in validClientIds)
            {
                ProducerConfig.ValidateClientId(validClientId);
            }
         }

        [Fact]
        public void TestInvalidGroupIds()
        {
            var badChars = new List<string> { "/", "\\", ",", "\0", ":", "\"", "\'", ";", "*", "?", " ", "\t", "\r", "\n", "=" };
            var invalidGroupIds = badChars.Select(weirdChar => "Is " + weirdChar + "illegal").ToList();

            foreach (string invalidGroupId in invalidGroupIds)
            {
                try
                {
                    ConsumerConfig.ValidateGroupId(invalidGroupId);
                    Assert.False(true);
                }
                catch
                {
                }
            }

            var validGroupIds = new List<string> { "valid", "GROUP", "iDs", "ar6", "VaL1d", "_0-9_.", string.Empty };

            foreach (string validGroupId in validGroupIds)
            {
                ConsumerConfig.ValidateGroupId(validGroupId);
            }
        }
    }
}