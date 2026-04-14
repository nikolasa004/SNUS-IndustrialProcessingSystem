namespace IndustrialProcessingSystem
{
    internal class Program
    {
        private static readonly SemaphoreSlim LogSemaphore = new SemaphoreSlim(1, 1);

        static void Main(string[] args)
        {
            try
            {
                string configPath = Path.Combine(AppContext.BaseDirectory, "SystemConfig.xml");
                SystemConfig config = SystemConfig.Load(configPath);

                Console.WriteLine("Config file loaded.");
                Console.WriteLine($"WorkerCount = {config.WorkerCount}");
                Console.WriteLine($"MaxQueueSize = {config.MaxQueueSize}");
                Console.WriteLine();

                using ProcessingSystem system = new ProcessingSystem(config.WorkerCount, config.MaxQueueSize);
                SubscribeToEvents(system);

                system.Start();

                Console.WriteLine("Loading initial jobs...");
                foreach (Job job in config.InitialJobs)
                {
                    try
                    {
                        JobHandle handle = system.Submit(job);
                        Console.WriteLine($"Initial job added: {job}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error adding initial job: {ex.Message}");
                    }
                }

                CancellationTokenSource cts = new CancellationTokenSource();
                List<Thread> producerThreads = StartProducerThreads(system, config.WorkerCount, cts.Token);

                Console.WriteLine();
                Console.WriteLine("System is running.");
                Console.WriteLine("Press ENTER to display top 5 jobs.");
                Console.WriteLine("Type 'q' and press ENTER to quit.");

                while (true)
                {
                    string? input = Console.ReadLine();

                    if (string.Equals(input, "q", StringComparison.OrdinalIgnoreCase))
                    {
                        break;
                    }

                    Console.WriteLine();
                    Console.WriteLine("Top 5 jobs in the queue:");

                    foreach (Job job in system.GetTopJobs(5))
                    {
                        Console.WriteLine(job);
                    }

                    Console.WriteLine();
                    Console.WriteLine("Press ENTER to display top 5 jobs again or 'q' to quit.");
                }

                cts.Cancel();

                foreach (Thread producer in producerThreads)
                {
                    if (producer.IsAlive)
                    {
                        producer.Join();
                    }
                }

                system.Stop();
                Console.WriteLine("System has been stopped.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Fatal error:");
                Console.WriteLine(ex.Message);
            }
        }

        private static void SubscribeToEvents(ProcessingSystem system)
        {
            string logPath = Path.Combine(AppContext.BaseDirectory, "job_log.txt");

            system.JobCompleted += async (_, e) =>
            {
                try
                {
                    string line = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] [{e.Status}] JobId={e.Job.Id}, Result={e.Result}, Attempt={e.Attempt}, DurationMs={e.Duration.TotalMilliseconds:F2}{Environment.NewLine}";
                    await AppendLogAsync(logPath, line);

                    Console.WriteLine($"COMPLETED -> {e.Job.Id}, Result={e.Result}, Attempt={e.Attempt}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error while logging COMPLETED job: {ex.Message}");
                }
            };

            system.JobFailed += async (_, e) =>
            {
                try
                {
                    string resultText = e.Result.HasValue ? e.Result.Value.ToString() : "N/A";
                    string messagePart = string.IsNullOrWhiteSpace(e.Message) ? "" : $", Message={e.Message}";
                    string line = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] [{e.Status}] JobId={e.Job.Id}, Result={resultText}, Attempt={e.Attempt}, DurationMs={e.Duration.TotalMilliseconds:F2}{messagePart}{Environment.NewLine}";
                    await AppendLogAsync(logPath, line);

                    Console.WriteLine($"{e.Status} -> {e.Job.Id}, Attempt={e.Attempt}, Reason={e.Message}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error while logging FAILED/ABORT job: {ex.Message}");
                }
            };
        }

        private static async Task AppendLogAsync(string path, string text)
        {
            string? directory = Path.GetDirectoryName(path);
            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(directory);
            }

            await LogSemaphore.WaitAsync();
            try
            {
                await File.AppendAllTextAsync(path, text);
            }
            finally
            {
                LogSemaphore.Release();
            }
        }

        private static List<Thread> StartProducerThreads(ProcessingSystem system, int producerCount, CancellationToken token)
        {
            List<Thread> producers = new List<Thread>();

            for (int i = 0; i < producerCount; i++)
            {
                Thread producer = new Thread(() => ProducerLoop(system, token))
                {
                    IsBackground = true,
                    Name = $"Producer-{i + 1}"
                };

                producers.Add(producer);
                producer.Start();
            }

            return producers;
        }

        private static void ProducerLoop(ProcessingSystem system, CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    Job job = CreateRandomJob();
                    JobHandle handle = system.Submit(job);

                    Console.WriteLine($"Producer added job: {job.Id} | Type={job.Type} | Priority={job.Priority} | Payload={job.Payload}");
                }
                catch (InvalidOperationException ex)
                {
                    Console.WriteLine($"Submit rejected: {ex.Message}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error in producer thread: {ex.Message}");
                }

                Thread.Sleep(Random.Shared.Next(300, 901));
            }
        }

        private static Job CreateRandomJob()
        {
            bool prime = Random.Shared.Next(0, 2) == 0;

            if (prime)
            {
                int number = Random.Shared.Next(5000, 30001);
                int threads = Random.Shared.Next(1, 9);

                return new Job
                {
                    Id = Guid.NewGuid(),
                    Type = JobType.Prime,
                    Payload = $"numbers:{number},threads:{threads}",
                    Priority = Random.Shared.Next(1, 6)
                };
            }

            int delay = Random.Shared.Next(500, 3501);

            // Chance to create a longer IO job to test timeouts and retries
            if (Random.Shared.Next(0, 5) == 0)
            {
                delay = Random.Shared.Next(2500, 5001);
            }

            return new Job
            {
                Id = Guid.NewGuid(),
                Type = JobType.IO,
                Payload = $"delay:{delay}",
                Priority = Random.Shared.Next(1, 6)
            };
        }
    }
}