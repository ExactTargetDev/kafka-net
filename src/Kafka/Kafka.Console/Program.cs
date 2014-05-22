namespace Kafka.Console
{
    using Kafka.Tests.Producers;

    using Console = System.Console;

    class Program
    {
        static void Main(string[] args)
        {


            for (var x = 0; x < 1; x++)
            {
                Console.WriteLine("RUN #" + x);
                using (var test = new ProducerTest())
                {
                    test.TestUpdateBrokerPartitionInfo();
                }

            }
        }
    }
}
