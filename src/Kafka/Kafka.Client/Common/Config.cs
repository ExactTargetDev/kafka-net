namespace Kafka.Client.Common
{
    using System.Configuration;
    using System.Text.RegularExpressions;

    public class Config
    {
        public static void ValidateChars(string prop, string value)
        {
            const string LegalChars = "[a-zA-Z0-9\\._\\-]";
            var rgx = new Regex(LegalChars + "*");

            if (!rgx.IsMatch(value))
            {
                throw new ConfigurationErrorsException(prop + " value " + value + " is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'");                
            }
        }
    }
}