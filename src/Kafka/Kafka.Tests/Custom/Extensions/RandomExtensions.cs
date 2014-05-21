namespace Kafka.Tests.Custom.Extensions
{
    using System;
    using System.Linq;

    public static class RandomExtensions
    {
         public static string NextString(this Random random, int length)
         {
             Func<char> safeChar = () =>
                 {
                     const int surrogateStart = 0xd800;
                     var res = random.Next(surrogateStart - 1) + 1;
                     return (char)res;
                 };

             return new string(Enumerable.Range(1, length).Select(x => safeChar()).ToArray());
         }
    }
}