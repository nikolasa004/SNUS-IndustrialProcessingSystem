namespace IndustrialProcessingSystem
{
    public class Job
    {
        public Guid Id { get; set; }
        public JobType Type { get; set; }
        public string Payload { get; set; }
        public int Priority { get; set; }

        public Job()
        {
            Payload = string.Empty;
        }

        public override string ToString()
        {
            return $"[{Id}] Type={Type}, Priority={Priority}, Payload={Payload}";
        }
    }
}
