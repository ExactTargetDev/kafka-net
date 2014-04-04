using System;
using System.Collections.Generic;

namespace Kafka.Client.Utils
{
    public class VerifiableProperties
    {
        private Dictionary<string, string> props;

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
            return props.ContainsKey(name);
        }

        public string GetProperty(string name)
        {
            var value = props[name];
            referenceSet.Add(value);
            return value;
        }

        public int GetInt(string name)
        {
            return Convert.ToInt32(GetString(name));
        }

        public int GetIntInRange(string name, Tuple<int, int> range)
        {
            if (props.ContainsKey(name) == false)
            {
                throw new ArgumentException("Missing required property", name);
            }
            return GetIntInRange(name, -1, range);
        }

        public int GetInt(string name, int @default)
        {
            return GetIntInRange(name, @default, Tuple.Create(int.MinValue, int.MaxValue));
        }

        public int GetIntInRange(string name, int @default, Tuple<int, int> range)
        {
            var v = (ContainsKey(name)) ? Convert.ToInt32(GetProperty(name)) : @default;

            if ((v >= range.Item1 && v<= range.Item2) == false)
            {
                throw new ArgumentException(name + " has value " + v + " which is not in the range " + range + ".", name);
            }
            return v;
        }

 // TODO:      /*
 //        * /**


 //def getShortInRange(name: String, default: Short, range: (Short, Short)): Short = {
 //   val v =
 //     if(containsKey(name))
 //       getProperty(name).toShort
 //     else
 //       default
 //   require(v >= range._1 && v <= range._2, name + " has value " + v + " which is not in the range " + range + ".")
 //   v
 // }

 // /**
 //  * Read a required long property value or throw an exception if no such property is found
 //  */
 // def getLong(name: String): Long = getString(name).toLong

 // /**
 //  * Read an long from the properties instance
 //  * @param name The property name
 //  * @param default The default value to use if the property is not found
 //  * @return the long value
 //  */
 // def getLong(name: String, default: Long): Long =
 //   getLongInRange(name, default, (Long.MinValue, Long.MaxValue))

 // /**
 //  * Read an long from the properties instance. Throw an exception
 //  * if the value is not in the given range (inclusive)
 //  * @param name The property name
 //  * @param default The default value to use if the property is not found
 //  * @param range The range in which the value must fall (inclusive)
 //  * @throws IllegalArgumentException If the value is not in the given range
 //  * @return the long value
 //  */
 // def getLongInRange(name: String, default: Long, range: (Long, Long)): Long = {
 //   val v =
 //     if(containsKey(name))
 //       getProperty(name).toLong
 //     else
 //       default
 //   require(v >= range._1 && v <= range._2, name + " has value " + v + " which is not in the range " + range + ".")
 //   v
 // }
  
 // /**
 //  * Get a required argument as a double
 //  * @param name The property name
 //  * @return the value
 //  * @throw IllegalArgumentException If the given property is not present
 //  */
 // def getDouble(name: String): Double = getString(name).toDouble
  
 // /**
 //  * Get an optional argument as a double
 //  * @param name The property name
 //  * @default The default value for the property if not present
 //  */
 // def getDouble(name: String, default: Double): Double = {
 //   if(containsKey(name))
 //     getDouble(name)
 //   else
 //     default
 // } 

 // /**
 //  * Read a boolean value from the properties instance
 //  * @param name The property name
 //  * @param default The default value to use if the property is not found
 //  * @return the boolean value
 //  */
 // def getBoolean(name: String, default: Boolean): Boolean = {
 //   if(!containsKey(name))
 //     default
 //   else {
 //     val v = getProperty(name)
 //     require(v == "true" || v == "false", "Unacceptable value for property '" + name + "', boolean values must be either 'true' or 'false")
 //     v.toBoolean
 //   }
 // }
  
 // def getBoolean(name: String) = getString(name).toBoolean


        public string GetString(string name, string @default)
        {
            return ContainsKey(name) ? GetProperty(name) : @default;
        }

        public string GetString(string name)
        {
            if (ContainsKey(name) == false)
            {
                throw new ArgumentException("Missing required property " + name, name);
            }
            return GetProperty(name);
        }

  
 // TODO: /**
 //  * Get a Map[String, String] from a property list in the form k1:v2, k2:v2, ...
 //  */
 // def getMap(name: String, valid: String => Boolean = s => true): Map[String, String] = {
 //   try {
 //     val m = Utils.parseCsvMap(getString(name, ""))
 //     m.foreach {
 //       case(key, value) => 
 //         if(!valid(value))
 //           throw new IllegalArgumentException("Invalid entry '%s' = '%s' for property '%s'".format(key, value, name))
 //     }
 //     m
 //   } catch {
 //     case e: Exception => throw new IllegalArgumentException("Error parsing configuration property '%s': %s".format(name, e.getMessage))
 //   }
 // }

 // def verify() {
 //   info("Verifying properties")
 //   val propNames = {
 //     import JavaConversions._
 //     Collections.list(props.propertyNames).map(_.toString).sorted
 //   }
 //   for(key <- propNames) {
 //     if (!referenceSet.contains(key) && !key.startsWith("external"))
 //       warn("Property %s is not valid".format(key))
 //     else
 //       info("Property %s is overridden to %s".format(key, props.getProperty(key)))
 //   }
 // }*/
    }
}