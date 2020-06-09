using System;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using osu.Framework.Threading;
using StackExchange.Redis;

namespace osu.Server.QueueProcessor
{
    public abstract class QueueProcessor<T> where T : QueueItem
    {
        private readonly QueueConfiguration config;

        /// <summary>
        /// An option queue to push to when finished.
        /// </summary>
        private readonly ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(
            Environment.GetEnvironmentVariable("REDIS_HOST") ?? "redis");

        private readonly string inputQueueName;

        private long totalProcessed;

        private long totalDequeued;

        private long totalInFlight => totalDequeued - totalProcessed;

        protected QueueProcessor(QueueConfiguration config)
        {
            this.config = config;

            const string queue_prefix = "osu-queue:";

            inputQueueName = $"{queue_prefix}{config.InputQueueName}";
        }

        /// <summary>
        /// Start running the queue.
        /// </summary>
        /// <param name="cancellation">An optional cancellation token.</param>
        public void Run(CancellationToken cancellation = default)
        {
            using (new Timer(_ => outputStats(), null, TimeSpan.Zero, TimeSpan.FromSeconds(5)))
            using (var cts = new GracefulShutdownSource(cancellation))
            {
                Console.WriteLine($"Starting queue processing (Backlog of {GetQueueSize()})..");

                using (var threadPool = new ThreadedTaskScheduler(Environment.ProcessorCount, "workers"))
                {
                    var database = redis.GetDatabase();

                    while (!cts.Token.IsCancellationRequested)
                    {
                        T item = null;

                        try
                        {
                            if (totalInFlight > config.MaxInFlightItems)
                            {
                                Thread.Sleep(config.TimeBetweenPolls);
                                continue;
                            }

                            var redisValue = database.ListRightPop(inputQueueName);

                            if (!redisValue.HasValue)
                            {
                                Thread.Sleep(config.TimeBetweenPolls);
                                continue;
                            }

                            Interlocked.Increment(ref totalDequeued);
                            item = JsonConvert.DeserializeObject<T>(redisValue);

                            // individual processing should not be cancelled as we have already grabbed from the queue.
                            Task.Factory.StartNew(() =>
                                {
                                    ProcessResult(item);
                                }, CancellationToken.None, TaskCreationOptions.HideScheduler, threadPool)
                                .ContinueWith(t =>
                                {
                                    Interlocked.Increment(ref totalProcessed);

                                    if (t.Exception != null)
                                    {
                                        Console.WriteLine($"Error processing {item}: {t.Exception}");
                                        attemptRetry(item);
                                    }
                                }, CancellationToken.None);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine($"Error processing from queue: {e}");
                            attemptRetry(item);
                        }
                    }

                    Console.WriteLine("Shutting down..");
                }
            }

            outputStats();
        }

        private void attemptRetry(T item)
        {
            if (item == null) return;

            if (item.TotalRetries++ < config.MaxRetries)
            {
                Console.WriteLine($"Re-queueing for attempt {item.TotalRetries} / {config.MaxRetries}");
                PushToQueue(item);
            }
            else
            {
                Console.WriteLine("Attempts exhausted; dropping item");
            }
        }

        private void outputStats()
        {
            Console.WriteLine($"stats: queue:{GetQueueSize()} inflight:{totalInFlight} dequeued:{totalDequeued} processed:{totalProcessed}");
        }

        public void PushToQueue(T obj) =>
            redis.GetDatabase().ListLeftPush(inputQueueName, JsonConvert.SerializeObject(obj));

        public long GetQueueSize() =>
            redis.GetDatabase().ListLength(inputQueueName);

        public void ClearQueue() => redis.GetDatabase().KeyDelete(inputQueueName);

        protected MySqlConnection GetDatabaseConnection()
        {
            string host = (Environment.GetEnvironmentVariable("DB_HOST") ?? "db");
            string user = (Environment.GetEnvironmentVariable("DB_USER") ?? "www");

            var connection = new MySqlConnection($"Server={host};Database=osu;User ID={user};ConnectionTimeout=5;");
            connection.Open();
            return connection;
        }

        /// <summary>
        /// Implement to process a single item from the queue.
        /// </summary>
        /// <param name="item">The item to process.</param>
        protected abstract void ProcessResult(T item);
    }
}
