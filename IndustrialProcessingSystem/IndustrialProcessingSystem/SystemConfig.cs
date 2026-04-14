using System.Globalization;
using System.Xml.Linq;

namespace IndustrialProcessingSystem
{
    public class SystemConfig
    {
        public int WorkerCount { get; set; }
        public int MaxQueueSize { get; set; }
        public List<Job> InitialJobs { get; set; }

        public SystemConfig()
        {
            InitialJobs = new List<Job>();
        }

        public static SystemConfig Load(string filePath)
        {
            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException("Config file not found.", filePath);
            }

            XDocument doc = XDocument.Load(filePath);
            XElement? root = doc.Element("SystemConfig");

            if (root == null)
            {
                throw new InvalidOperationException("Wrong XML format: missing SystemConfig element.");
            }

            SystemConfig config = new SystemConfig
            {
                WorkerCount = ParseInt(root.Element("WorkerCount")?.Value, "WorkerCount"),
                MaxQueueSize = ParseInt(root.Element("MaxQueueSize")?.Value, "MaxQueueSize")
            };

            XElement? jobsElement = root.Element("Jobs");
            if (jobsElement != null)
            {
                foreach (XElement jobElement in jobsElement.Elements("Job"))
                {
                    XAttribute? typeAttr = jobElement.Attribute("Type");
                    XAttribute? payloadAttr = jobElement.Attribute("Payload");
                    XAttribute? priorityAttr = jobElement.Attribute("Priority");

                    if (typeAttr == null || payloadAttr == null || priorityAttr == null)
                    {
                        throw new InvalidOperationException("Each Job in XML must have Type, Payload, and Priority attributes.");
                    }

                    if (!Enum.TryParse(typeAttr.Value, true, out JobType type))
                    {
                        throw new InvalidOperationException($"Unknown JobType: {typeAttr.Value}");
                    }

                    Job job = new Job
                    {
                        Id = Guid.NewGuid(),
                        Type = type,
                        Payload = payloadAttr.Value,
                        Priority = ParseInt(priorityAttr.Value, "Priority")
                    };

                    config.InitialJobs.Add(job);
                }
            }

            return config;
        }

        private static int ParseInt(string? value, string fieldName)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new InvalidOperationException($"Missing value for {fieldName}.");
            }

            string cleaned = value.Replace("_", "").Trim();

            if (!int.TryParse(cleaned, NumberStyles.Integer, CultureInfo.InvariantCulture, out int result))
            {
                throw new InvalidOperationException($"Invalid int value for {fieldName}: {value}");
            }

            return result;
        }
    }
}