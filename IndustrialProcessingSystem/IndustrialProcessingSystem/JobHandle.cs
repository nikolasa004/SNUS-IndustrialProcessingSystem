namespace IndustrialProcessingSystem
{
    public class JobHandle
    {
        public Guid Id { get; set; }
        public Task<int> Result { get; set; }

        public JobHandle()
        {
            Result = Task.FromResult(0);
        }
    }
}
