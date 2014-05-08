namespace Kafka.Client.Metrics
{
    using System;

    using Kafka.Client.Common.Imported;

    /// <summary>
    ///  A wrapper around metrics timer object that provides a convenient mechanism
    /// to time code blocks. This pattern was borrowed from the metrics-scala_2.9.1
    /// package.
    /// </summary>
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