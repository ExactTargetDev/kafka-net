namespace Kafka.Tests
{
    using System;
    using System.Collections.Generic;

    using Kafka.Client.Utils;

    using Xunit;

    public class IteratorTemplateTests
    {
        [Fact]
        public void SimpleTest()
        {
            var iter = new MyIterator();
            while (iter.MoveNext())
            {
                Console.WriteLine(iter.Current);
                //TODO: put to array and compare
            }
        }
    }

    public class MyIterator : IteratorTemplate<string>
    {
        private int i = -1;

        private List<string> items = new List<string> {"1", "2", "3"};  

        protected override string MakeNext()
        {
            i++;
            if (i < items.Count)
            {
                return items[i];
            }
            this.AllDone();
            return null;
        }
    }
}