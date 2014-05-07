namespace Kafka.Client.ZKClient
{
    using System;
    using System.Collections.Concurrent;
    using System.Reflection;
    using System.Threading;

    using Kafka.Client.Common.Imported;

    using log4net;

    public class ZkEventThread 
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);       

        private BlockingCollection<ZkEvent> _events = new BlockingCollection<ZkEvent>();

        private AtomicInteger _eventId = new AtomicInteger(0);

        public string Name { get; private set; }

        private CancellationTokenSource tokenSource = new CancellationTokenSource();

        public Thread Thread { get; private set; }

        public ZkEventThread(string name)
        {
            this.Name = name;
        }

        public void Start()
        {
            this.Thread = new Thread(this.Run);
            this.Thread.Name = this.Name;
            this.Thread.IsBackground = true;
            this.Thread.Start();
        }

        public void Run()
        {
            Logger.Info("Starting ZkClient event thread.");
            try
            {
                while (!this.tokenSource.IsCancellationRequested)
                {
                    ZkEvent zkEvent = this._events.Take();
                    int eventId = this._eventId.GetAndIncrement();
                    Logger.Debug("Delivering event #" + eventId + " " + zkEvent);
                    try
                    {
                        zkEvent.RunAction();
                    }
                    catch (ThreadInterruptedException)
                    {
                        this.tokenSource.Cancel();
                    }
                    catch (Exception e)
                    {
                        Logger.Error("Error handling event " + zkEvent, e);
                    }

                    Logger.Debug("Delivering event #" + eventId + " done");
                }
            }
            catch (ThreadInterruptedException)
            {
                Logger.Info("Terminate ZkClient event thread.");
            }
        }

        public void Send(ZkEvent zkEvent) 
        {
            if (!this.tokenSource.IsCancellationRequested) 
            {
                Logger.Debug("New event: " + zkEvent);
                this._events.Add(zkEvent);
            }
        }

        public void Interrupt()
        {
            this.Thread.Interrupt();
        }

        public void Join(int i)
        {
            this.Thread.Join(i);
        }
    }
}