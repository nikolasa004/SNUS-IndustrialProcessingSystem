namespace IndustrialProcessingSystem
{
    public class JobEventArgs : EventArgs
    {
        public Job Job { get; }
        public int? Result { get; }
        public string Status { get; }
        public int Attempt { get; }
        public TimeSpan Duration { get; }
        public string Message { get; }

        public JobEventArgs(Job job, int? result, string status, int attempt, TimeSpan duration, string message = "")
        {
            Job = job;
            Result = result;
            Status = status;
            Attempt = attempt;
            Duration = duration;
            Message = message;
        }
    }
}
