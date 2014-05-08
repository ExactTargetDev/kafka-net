namespace Kafka.Client.Utils
{
    using System;

    /// <summary>
    /// A scheduler for running jobs
    /// 
    /// This interface controls a job scheduler that allows scheduling either repeating background jobs
    /// that execute periodically or delayed one-time actions that are scheduled in the future.
    /// </summary>
    public interface IScheduler
    {
        /// <summary>
        /// Initialize this scheduler so it is ready to accept scheduling of tasks
        /// </summary>
        void Startup();

        /// <summary>
        /// Shutdown this scheduler. When this method is complete no more executions of background tasks will occur.
        /// This includes tasks scheduled with a delayed execution.
        /// </summary>
        void Shutdown();

        /// <summary>
        /// Schedule a task
        /// </summary>
        /// <param name="name">The name of this task</param>
        /// <param name="action">The action to be performed</param>
        /// <param name="delay">The amount of time to wait before the first execution</param>
        /// <param name="period">The period with which to execute the task. If &lt; 0 the task will execute only once.</param>
        void Schedule(string name, Action action, TimeSpan? delay, TimeSpan? period);
    }
}