namespace Kafka.Client.ZKClient
{
    using System;
    using System.Collections.Concurrent;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;

    using Kafka.Client.Common.Imported;
    using Kafka.Client.ZKClient.Exceptions;

    using log4net;

    public class ZkEventThread 
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);       

        private BlockingCollection<ZkEvent> _events = new BlockingCollection<ZkEvent>();

        private AtomicInteger _eventId = new AtomicInteger(0);

        public string Name { get; private set; }

        private CancellationTokenSource tokenSource = new CancellationTokenSource();

        private CancellationToken token;

        public Thread thread { get; private set; }

        public ZkEventThread(string name)
        {
            this.Name = name;
            token = tokenSource.Token;
        }

        public void Start()
        {
            this.thread = new Thread(this.Run);
            this.thread.Name = this.Name;
            this.thread.IsBackground = true;
            this.thread.Start();
        }

        public void Run()
        {
            Logger.Info("Starting ZkClient event thread.");
            try
            {
                while (!tokenSource.IsCancellationRequested)
                {
                    ZkEvent zkEvent = _events.Take();
                    int eventId = _eventId.GetAndIncrement();
                    Logger.Debug("Delivering event #" + eventId + " " + zkEvent);
                    try
                    {
                        zkEvent.RunAction();
                    }
                    catch (ThreadInterruptedException e)
                    {
                        tokenSource.Cancel();
                    }
                    catch (Exception e)
                    {
                        Logger.Error("Error handling event " + zkEvent, e);
                    }
                    Logger.Debug("Delivering event #" + eventId + " done");
                }
            }
            catch (ThreadInterruptedException e)
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
            this.thread.Interrupt();
        }

        public void Join(int i)
        {
            this.thread.Join(i);
        }
    }
}