namespace Kafka.Client.Extensions
{
    using System;

    public static class DateTimeHelper
    {
         public static long CurrentTimeMilis()
         {
             return (long)(DateTime.Now - new DateTime(1970, 1, 1)).TotalMilliseconds;
         }
    }
}