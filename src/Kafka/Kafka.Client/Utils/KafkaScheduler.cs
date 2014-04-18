namespace Kafka.Client.Utils
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;

    using log4net;

    public class KafkaScheduler : IScheduler
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly List<Timer> timers = new List<Timer>();

        private readonly List<Task> tasks = new List<Task>();

        public void Startup()
        {
            Logger.Debug("Initializing task scheduler.");
        }

        public void Shutdown()
        {
            lock (this)
            {
                foreach (var task in this.tasks)
                {
                    task.Dispose();
                }

                foreach (var timer in this.timers)
                {
                    timer.Dispose();
                }
            }
        }

        public void Schedule(string name, Action action, TimeSpan? delay, TimeSpan? period)
        {
            if (delay.HasValue == false)
            {
                delay = TimeSpan.Zero;
            }

            lock (this)
            {
                TimerCallback callback = state =>
                    {
                        try
                        {
                            Logger.DebugFormat("Begining execution of scheduled task {0}", name);
                            action();
                        }
                        catch (Exception e)
                        {
                            Logger.Error("Uncaught exception in scheduled task " + name, e);
                        }
                        finally
                        {
                            Logger.DebugFormat("Completed execution of scheduled task {0}", name);
                        }
                    };

                if (period.HasValue)
                {
                    var timer = new Timer(callback, null, delay.Value, period.Value);
                    this.timers.Add(timer);
                }
                else
                {
                    var task = Task.Factory.StartNew(() => Thread.Sleep(delay.Value));
                    task.ContinueWith(result => callback(null));
                    this.tasks.Add(task);
                }
            }
        }
    }
}