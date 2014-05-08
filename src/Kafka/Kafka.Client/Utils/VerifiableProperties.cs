namespace Kafka.Client.Utils
{
    using System;
    using System.Collections.Generic;

    public class VerifiableProperties
    {
        private readonly Dictionary<string, string> props;

        private readonly HashSet<string> referenceSet = new HashSet<string>();

        public VerifiableProperties() : this(new Dictionary<string, string>())
        {
        }

        public VerifiableProperties(Dictionary<string, string> props)
        {
            this.props = props;
        }

        public bool ContainsKey(string name)
        {
            return this.props.ContainsKey(name);
        }

        public string GetProperty(string name)
        {
            var value = this.props[name];
            this.referenceSet.Add(value);
            return value;
        }

        public int GetInt(string name)
        {
            return Convert.ToInt32(this.GetString(name));
        }

        public int GetIntInRange(string name, Tuple<int, int> range)
        {
            if (this.props.ContainsKey(name) == false)
            {
                throw new ArgumentException("Missing required property", name);
            }

            return this.GetIntInRange(name, -1, range);
        }

        public int GetInt(string name, int @default)
        {
            return this.GetIntInRange(name, @default, Tuple.Create(int.MinValue, int.MaxValue));
        }

        public int GetIntInRange(string name, int @default, Tuple<int, int> range)
        {
            var v = this.ContainsKey(name) ? Convert.ToInt32(this.GetProperty(name)) : @default;

            if ((v >= range.Item1 && v <= range.Item2) == false)
            {
                throw new ArgumentException(name + " has value " + v + " which is not in the range " + range + ".", name);
            }

            return v;
        }

        public string GetString(string name, string @default)
        {
            return this.ContainsKey(name) ? this.GetProperty(name) : @default;
        }

        public string GetString(string name)
        {
            if (this.ContainsKey(name) == false)
            {
                throw new ArgumentException("Missing required property " + name, name);
            }

            return this.GetProperty(name);
        }
    }
}