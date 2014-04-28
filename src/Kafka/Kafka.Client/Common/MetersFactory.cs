namespace Kafka.Client.Common
{
    using System;

    using Kafka.Client.Common.Imported;

    public static class MetersFactory
    {
         public static IMeter NewMeter(string id, string eventType, TimeSpan unit)
         {
             return new NoOpMeter();
         }
    }

    internal class NoOpMeter : IMeter
    {
        public void Mark()
        {
            // no -op
        }

        public void Mark(int value)
        {
            // no -op
        }
    }
}