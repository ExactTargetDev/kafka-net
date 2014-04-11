namespace Kafka.Tests.Utils
{
    using System.Collections.Generic;

    using Kafka.Client.Messages;
    using Kafka.Client.Utils;

    using Xunit;

    public static class TestUtils
    {
        class MessageIterator : IteratorTemplate<Message>
        {
            private IEnumerator<MessageAndOffset> iter;

            public MessageIterator(IEnumerator<MessageAndOffset> iter)
            {
                this.iter = iter;
            }

            protected override Message MakeNext()
            {
                if (this.iter.MoveNext())
                {
                    return iter.Current.Message;
                }
                return this.AllDone();
            }
        }

         public static IEnumerator<Message> GetMessageIterator(IEnumerator<MessageAndOffset> iter)
         {
             return new MessageIterator(iter);
         }

        /// <summary>
         /// Throw an exception if the two iterators are of differing lengths or contain
         /// different messages on their Nth element
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="expected"></param>
        /// <param name="actual"></param>
        public static void CheckEquals<T>(IEnumerator<T> expected, IEnumerator<T> actual)
        {
            var length = 0;
            while (expected.MoveNext() && actual.MoveNext())
            {
                length++;
                Assert.Equal(expected.Current, actual.Current);
            }

            // check if the expected iterator is longer
            if (expected.MoveNext())
            {
                var length1 = length;
                while (expected.MoveNext())
                {
                    var current = expected.Current;
                    length1++;
                }
                Assert.False(false, "Iterators have uneven length -- first has more " + length1 + " > " + length);
            }

            // check if the actual iterator was longer
            if (actual.MoveNext())
            {
                var length2 = length;
                while (actual.MoveNext())
                {
                    var current = actual.Current;
                    length2++;
                }
                Assert.False(false, "Iterators have uneven length -- second has more " + length2 + " > " + length);
            }

        }
    }
}