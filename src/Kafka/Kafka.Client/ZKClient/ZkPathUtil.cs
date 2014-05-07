namespace Kafka.Client.ZKClient
{
    using System;
    using System.Text;

    public class ZkPathUtil
    {
        public static string ToString(ZkClient zkClient)
        {
            return ToString(zkClient, "/", s => true);
        }

        public static string ToString(ZkClient zkClient, string startPath, Func<string, bool> pathFilter) 
        {
            int level = 1;
            var builder = new StringBuilder("+ (" + startPath + ")");
            builder.Append("\n");
            AddChildrenToStringBuilder(zkClient, pathFilter, level, builder, startPath);
            return builder.ToString();
        }

        private static void AddChildrenToStringBuilder(ZkClient zkClient, Func<string, bool> pathFilter, int level, StringBuilder builder, string startPath)
        {
            var children = zkClient.GetChildren(startPath);
            foreach (var node in children) 
            {
                string nestedPath;
                if (startPath.EndsWith("/")) 
                {
                    nestedPath = startPath + node;
                } 
                else
                {
                    nestedPath = startPath + "/" + node;
                }

                if (pathFilter(nestedPath)) 
                {
                    builder.Append(GetSpaces(level - 1) + "'-" + "+" + node + "\n");
                    AddChildrenToStringBuilder(zkClient, pathFilter, level + 1, builder, nestedPath);
                }
                else
                {
                    builder.Append(GetSpaces(level - 1) + "'-" + "-" + node + " (contents hidden)\n");
                }
            }
        }

        private static string GetSpaces(int level)
        {
            var s = string.Empty;
            for (var i = 0; i < level; i++) 
            {
                s += "  ";
            }

            return s;
        }
    }
}