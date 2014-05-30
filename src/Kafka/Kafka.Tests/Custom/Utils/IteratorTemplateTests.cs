namespace Kafka.Tests.Custom.Utils
{
    using System.Collections.Generic;
    using System.Linq;

    using Kafka.Client.Extensions;
    using Kafka.Client.Utils;

    using Xunit;

    public class IteratorTemplateTests
    {
        [Fact]
        public void SimpleTest()
        {
            var iter = new MyIterator();
            var result = iter.ToEnumerable().ToList();
            var expected = new List<string> { "1", "2", "3" };
            Assert.Equal(expected, result);
        }
    }

    public class MyIterator : IteratorTemplate<string>
    {
        private int i = -1;

        private readonly List<string> items = new List<string> { "1", "2", "3" };  

        protected override string MakeNext()
        {
            this.i++;
            if (this.i < this.items.Count)
            {
                return this.items[this.i];
            }

            this.AllDone();
            return null;
        }
    }
}