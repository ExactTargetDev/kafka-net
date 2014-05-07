namespace Kafka.Client.Metrics
{
    using System;

    using Kafka.Client.Common.Imported;

    public class KafkaTimer
    {
        private readonly ITimer timer;

        public KafkaTimer(ITimer metric)
        {
            this.timer = metric;
        } 

        public void Time(Action f)
        {
            var ctx = this.timer.Time();
            try
            {
                f();
            }
            finally
            {
                ctx.Stop();
            }
        }
    }
}