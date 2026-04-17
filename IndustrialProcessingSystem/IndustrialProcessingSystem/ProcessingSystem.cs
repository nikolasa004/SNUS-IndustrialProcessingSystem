using System.Diagnostics;
using System.Xml.Linq;

namespace IndustrialProcessingSystem
{
    public class ProcessingSystem : IDisposable
    {
        public event EventHandler<JobEventArgs>? JobCompleted;
        public event EventHandler<JobEventArgs>? JobFailed;

        private readonly int _workerCount;
        private readonly int _maxQueueSize;

        private readonly object _lock = new object();
        private readonly SemaphoreSlim _queueSignal = new SemaphoreSlim(0);

        private readonly SortedDictionary<int, Queue<Job>> _priorityQueues = new SortedDictionary<int, Queue<Job>>();
        private readonly Dictionary<Guid, Job> _allJobs = new Dictionary<Guid, Job>();
        private readonly Dictionary<Guid, InternalJobState> _jobStates = new Dictionary<Guid, InternalJobState>();
        private readonly HashSet<Guid> _submittedJobIds = new HashSet<Guid>();
        private readonly List<JobExecutionRecord> _executionRecords = new List<JobExecutionRecord>();

        private readonly List<Thread> _workers = new List<Thread>();
        private readonly string _reportDirectory;
        private readonly Timer _reportTimer;

        private volatile bool _running;
        private int _pendingCount;

        public ProcessingSystem(int workerCount, int maxQueueSize, string reportDirectory = "Reports")
        {
            if (workerCount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(workerCount), "WorkerCount must be greater than 0.");
            }

            if (maxQueueSize <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxQueueSize), "MaxQueueSize must be greater than 0.");
            }

            _workerCount = workerCount;
            _maxQueueSize = maxQueueSize;
            _reportDirectory = Path.GetFullPath(reportDirectory);

            Directory.CreateDirectory(_reportDirectory);

            _reportTimer = new Timer(GenerateReportCallback, null, Timeout.Infinite, Timeout.Infinite);
        }

        public void Start()
        {
            if (_running)
            {
                return;
            }

            _running = true;

            for (int i = 0; i < _workerCount; i++)
            {
                Thread worker = new Thread(WorkerLoop)
                {
                    IsBackground = true,
                    Name = $"Worker-{i + 1}"
                };

                _workers.Add(worker);
                worker.Start();
            }

            _reportTimer.Change(TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }

        public void Stop()
        {
            if (!_running)
            {
                return;
            }

            _running = false;

            for (int i = 0; i < _workerCount; i++)
            {
                _queueSignal.Release();
            }

            foreach (Thread worker in _workers)
            {
                if (worker.IsAlive)
                {
                    worker.Join();
                }
            }

            _reportTimer.Change(Timeout.Infinite, Timeout.Infinite);
        }

        public JobHandle Submit(Job job)
        {
            if (job == null)
            {
                throw new ArgumentNullException(nameof(job));
            }

            if (job.Id == Guid.Empty)
            {
                job.Id = Guid.NewGuid();
            }

            if (job.Priority < 0)
            {
                throw new ArgumentException("Priority must not be negative.");
            }

            lock (_lock)
            {
                if (_submittedJobIds.Contains(job.Id))
                {
                    return _jobStates[job.Id].Handle;
                }

                if (_pendingCount >= _maxQueueSize)
                {
                    throw new InvalidOperationException("Queue is full. New job is rejected.");
                }

                _submittedJobIds.Add(job.Id);
                _allJobs[job.Id] = job;

                TaskCompletionSource<int> tcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
                JobHandle handle = new JobHandle
                {
                    Id = job.Id,
                    Result = tcs.Task
                };

                _jobStates[job.Id] = new InternalJobState
                {
                    Handle = handle,
                    CompletionSource = tcs
                };

                if (!_priorityQueues.ContainsKey(job.Priority))
                {
                    _priorityQueues[job.Priority] = new Queue<Job>();
                }

                _priorityQueues[job.Priority].Enqueue(job);
                _pendingCount++;

                _queueSignal.Release();

                return handle;
            }
        }

        public IEnumerable<Job> GetTopJobs(int n)
        {
            if (n <= 0)
            {
                return Enumerable.Empty<Job>();
            }

            lock (_lock)
            {
                return _priorityQueues
                    .OrderBy(kvp => kvp.Key)
                    .SelectMany(kvp => kvp.Value)
                    .Take(n)
                    .ToList();
            }
        }

        public Job GetJob(Guid id)
        {
            lock (_lock)
            {
                if (_allJobs.TryGetValue(id, out Job? job))
                {
                    return job;
                }
            }

            throw new KeyNotFoundException($"Job with ID {id} does not exist.");
        }

        private void WorkerLoop()
        {
            while (true)
            {
                _queueSignal.Wait();

                if (!_running)
                {
                    break;
                }

                Job? job = TryDequeue();
                if (job == null)
                {
                    continue;
                }

                try
                {
                    ExecuteJobWithRetryAsync(job).GetAwaiter().GetResult();
                }
                catch
                {
                    //  Ignore exceptions from job execution to keep worker running. Failures are handled in ExecuteJobWithRetryAsync.
                }
            }
        }

        private Job? TryDequeue()
        {
            lock (_lock)
            {
                foreach (KeyValuePair<int, Queue<Job>> pair in _priorityQueues.OrderBy(p => p.Key).ToList())
                {
                    if (pair.Value.Count > 0)
                    {
                        Job job = pair.Value.Dequeue();
                        _pendingCount--;

                        if (pair.Value.Count == 0)
                        {
                            _priorityQueues.Remove(pair.Key);
                        }

                        return job;
                    }
                }
            }

            return null;
        }

        private async Task ExecuteJobWithRetryAsync(Job job)
        {
            InternalJobState state;

            lock (_lock)
            {
                state = _jobStates[job.Id];
            }

            const int maxAttempts = 3;

            for (int attempt = 1; attempt <= maxAttempts; attempt++)
            {
                Stopwatch sw = Stopwatch.StartNew();

                try
                {
                    Task<int> processingTask = Task.Run(() => ProcessJob(job));
                    Task finishedTask = await Task.WhenAny(processingTask, Task.Delay(TimeSpan.FromSeconds(2)));

                    sw.Stop();

                    if (finishedTask != processingTask)
                    {
                        string timeoutMessage = $"Job has exceeded the time limit on attempt {attempt}.";
                        string status = attempt == maxAttempts ? "ABORT" : "FAILED";

                        OnJobFailed(new JobEventArgs(job, null, status, attempt, sw.Elapsed, timeoutMessage));

                        if (attempt == maxAttempts)
                        {
                            state.CompletionSource.TrySetException(new TimeoutException(timeoutMessage));
                            AddExecutionRecord(job, false, sw.Elapsed, status);
                            return;
                        }

                        continue;
                    }

                    int result = await processingTask;

                    state.CompletionSource.TrySetResult(result);
                    AddExecutionRecord(job, true, sw.Elapsed, "COMPLETED");
                    OnJobCompleted(new JobEventArgs(job, result, "COMPLETED", attempt, sw.Elapsed));

                    return;
                }
                catch (Exception ex)
                {
                    sw.Stop();

                    string status = attempt == maxAttempts ? "ABORT" : "FAILED";
                    OnJobFailed(new JobEventArgs(job, null, status, attempt, sw.Elapsed, ex.Message));

                    if (attempt == maxAttempts)
                    {
                        state.CompletionSource.TrySetException(ex);
                        AddExecutionRecord(job, false, sw.Elapsed, status);
                        return;
                    }
                }
            }
        }

        private int ProcessJob(Job job)
        {
            return job.Type switch
            {
                JobType.Prime => ProcessPrimeJob(job.Payload),
                JobType.IO => ProcessIoJob(job.Payload),
                _ => throw new InvalidOperationException($"Unsupported job type: {job.Type}")
            };
        }

        private int ProcessPrimeJob(string payload)
        {
            Dictionary<string, string> values = ParsePayload(payload);

            if (!values.TryGetValue("numbers", out string? numbersRaw))
            {
                throw new InvalidOperationException("Prime payload must contain 'numbers'.");
            }

            if (!values.TryGetValue("threads", out string? threadsRaw))
            {
                throw new InvalidOperationException("Prime payload must contain 'threads'.");
            }

            int maxNumber = ParsePayloadInt(numbersRaw);
            int threadCount = ParsePayloadInt(threadsRaw);

            if (maxNumber < 2)
            {
                return 0;
            }

            threadCount = Math.Clamp(threadCount, 1, 8);

            int totalCount = 0;
            object countLock = new object();

            List<Thread> threads = new List<Thread>();

            int chunkSize = maxNumber / threadCount;
            int currentStart = 2;

            for (int i = 0; i < threadCount; i++)
            {
                int localStart = currentStart;
                int localEnd = (i == threadCount - 1) ? maxNumber : (currentStart + chunkSize - 1);

                Thread thread = new Thread(() =>
                {
                    int localCount = 0;

                    for (int n = localStart; n <= localEnd; n++)
                    {
                        if (IsPrime(n))
                        {
                            localCount++;
                        }
                    }

                    lock (countLock)
                    {
                        totalCount += localCount;
                    }
                });

                thread.IsBackground = true;
                threads.Add(thread);
                thread.Start();

                currentStart = localEnd + 1;
            }

            foreach (Thread thread in threads)
            {
                thread.Join();
            }

            return totalCount;
        }

        private int ProcessIoJob(string payload)
        {
            Dictionary<string, string> values = ParsePayload(payload);

            if (!values.TryGetValue("delay", out string? delayRaw))
            {
                throw new InvalidOperationException("IO payload must contain 'delay'.");
            }

            int delay = ParsePayloadInt(delayRaw);
            Thread.Sleep(delay);

            return Random.Shared.Next(0, 101);
        }

        private static Dictionary<string, string> ParsePayload(string payload)
        {
            Dictionary<string, string> result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            string[] parts = payload.Split(',', StringSplitOptions.RemoveEmptyEntries);

            foreach (string part in parts)
            {
                string[] keyValue = part.Split(':', StringSplitOptions.RemoveEmptyEntries);

                if (keyValue.Length != 2)
                {
                    throw new InvalidOperationException($"Invalid payload part: {part}");
                }

                result[keyValue[0].Trim()] = keyValue[1].Trim();
            }

            return result;
        }

        private static int ParsePayloadInt(string raw)
        {
            string cleaned = raw.Replace("_", "").Trim();

            if (!int.TryParse(cleaned, out int value))
            {
                throw new InvalidOperationException($"Invalid numeric value in payload: {raw}");
            }

            return value;
        }

        private static bool IsPrime(int number)
        {
            if (number < 2)
            {
                return false;
            }

            if (number == 2)
            {
                return true;
            }

            if (number % 2 == 0)
            {
                return false;
            }

            int limit = (int)Math.Sqrt(number);

            for (int i = 3; i <= limit; i += 2)
            {
                if (number % i == 0)
                {
                    return false;
                }
            }

            return true;
        }

        private void AddExecutionRecord(Job job, bool success, TimeSpan duration, string status)
        {
            lock (_lock)
            {
                _executionRecords.Add(new JobExecutionRecord
                {
                    JobId = job.Id,
                    Type = job.Type,
                    Success = success,
                    Duration = duration,
                    Status = status,
                    Timestamp = DateTime.Now
                });
            }
        }

        private void GenerateReportCallback(object? state)
        {
            try
            {
                List<JobExecutionRecord> snapshot;

                lock (_lock)
                {
                    snapshot = _executionRecords.ToList();
                }

                DateTime now = DateTime.Now;

                var executedByType = snapshot
                    .GroupBy(r => r.Type)
                    .OrderBy(g => g.Key.ToString())
                    .Select(g => new
                    {
                        Type = g.Key.ToString(),
                        Count = g.Count()
                    });

                var averageDurationByType = snapshot
                    .Where(r => r.Success)
                    .GroupBy(r => r.Type)
                    .OrderBy(g => g.Key.ToString())
                    .Select(g => new
                    {
                        Type = g.Key.ToString(),
                        AverageMilliseconds = g.Average(x => x.Duration.TotalMilliseconds)
                    });

                var failedByType = snapshot
                    .Where(r => !r.Success)
                    .GroupBy(r => r.Type)
                    .OrderBy(g => g.Key.ToString())
                    .Select(g => new
                    {
                        Type = g.Key.ToString(),
                        Count = g.Count()
                    });

                XElement root = new XElement("Report",
                    new XAttribute("GeneratedAt", now.ToString("yyyy-MM-dd HH:mm:ss")),
                    new XElement("ExecutedJobsByType",
                        executedByType.Select(x =>
                            new XElement("Type",
                                new XAttribute("Name", x.Type),
                                new XAttribute("Count", x.Count)
                            )
                        )
                    ),
                    new XElement("AverageExecutionTimeByType",
                        averageDurationByType.Select(x =>
                            new XElement("Type",
                                new XAttribute("Name", x.Type),
                                new XAttribute("AverageMilliseconds", Math.Round(x.AverageMilliseconds, 2))
                            )
                        )
                    ),
                    new XElement("FailedJobsByType",
                        failedByType.Select(x =>
                            new XElement("Type",
                                new XAttribute("Name", x.Type),
                                new XAttribute("Count", x.Count)
                            )
                        )
                    )
                );

                string fullPath;

                string[] existingReports = Directory.GetFiles(_reportDirectory, "report_*.xml");

                if (existingReports.Length < 10)
                {
                    int nextIndex = existingReports.Length;
                    string fileName = $"report_{nextIndex}.xml";
                    fullPath = Path.Combine(_reportDirectory, fileName);
                }
                else
                {
                    fullPath = existingReports
                        .OrderBy(path => File.GetLastWriteTime(path))
                        .First();
                }

                root.Save(fullPath);
            }
            catch
            {
                // Ignore exceptions from report generation to avoid affecting main processing
            }
        }

        private void OnJobCompleted(JobEventArgs e)
        {
            JobCompleted?.Invoke(this, e);
        }

        private void OnJobFailed(JobEventArgs e)
        {
            JobFailed?.Invoke(this, e);
        }

        public void Dispose()
        {
            Stop();
            _queueSignal.Dispose();
            _reportTimer.Dispose();
        }

        private class InternalJobState
        {
            public JobHandle Handle { get; set; } = new JobHandle();
            public TaskCompletionSource<int> CompletionSource { get; set; } = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        private class JobExecutionRecord
        {
            public Guid JobId { get; set; }
            public JobType Type { get; set; }
            public bool Success { get; set; }
            public TimeSpan Duration { get; set; }
            public string Status { get; set; } = string.Empty;
            public DateTime Timestamp { get; set; }
        }
    }
}