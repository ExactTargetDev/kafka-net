namespace Kafka.Client.Utils
{
    using System;
    using System.Reflection;
    using System.Threading;

    using Kafka.Client.Common.Imported;

    using log4net;

    public abstract class ShutdownableThread
    {
        protected static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        protected string name;

        protected bool isInterruptible;

        private Thread innerThread;

        protected AtomicBoolean isRunning = new AtomicBoolean(true);

        private ManualResetEventSlim shutdownLatch = new ManualResetEventSlim(false);

        public ShutdownableThread(string name, bool isInterruptible = true)
        {
            this.name = name;
            this.isInterruptible = isInterruptible;
            this.innerThread = new Thread(this.Run);
            this.innerThread.IsBackground = false;

        }

        public virtual void Shutdown()
        {
            Logger.InfoFormat("Shutting down");
            isRunning.Set(false);
            if (isInterruptible)
            {
                this.innerThread.Interrupt();
            }
            this.shutdownLatch.Wait();
            Logger.InfoFormat("Shutdown completed");
        }

        public void Start()
        {
            this.innerThread.Start();
        }

        public void AwaitShutdown()
        {
            this.shutdownLatch.Wait();
        }

        public abstract void DoWork();

        public void Run()
        {
            Logger.Info("Starting");
            try
            {
                while (this.isRunning.Get())
                {
                    this.DoWork();
                }
            }
            catch (Exception e)
            {
                if (this.isRunning.Get())
                {
                    Logger.Error("Error due to", e);
                }
            }
            this.shutdownLatch.Set();
            Logger.Info("Stopped");
        }
    }
}