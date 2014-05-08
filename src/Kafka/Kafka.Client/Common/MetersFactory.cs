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

        public static ITimer NewTimer(string name, TimeSpan durationUnit, TimeSpan rateUnit)
        {
            return new NoOpTimer();
        }

        public static IHistogram NewHistogram(string name)
        {
            return new NoOpHistogram();
        }

        public static IGauge<T> NewGauge<T>(string name, Func<T> func)
        {
            return new NoOpGauge<T>();
        }
    }

    internal class NoOpHistogram : IHistogram
    {
        public void Update(int value)
        {
            // no -op
        }
    }

    internal class NoOpTimer : ITimer
    {
        public ITiming Time()
        {
            return new NoOpTiming();
        }
    }

    internal class NoOpTiming : ITiming
    {
        public void Stop()
        {
            // no-op
        }
    }

    internal class NoOpGauge<T> : IGauge<T>
    {
        public T Value 
        { 
            get
            {
                return default(T);
            } 
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