namespace Kafka.Tests
{
    using System;

    public class Program
    {
        static int Main(string[] args)
        {
            new ProducerTest().Produce();

            Console.ReadKey();
            return 0;
        } 
    }
}