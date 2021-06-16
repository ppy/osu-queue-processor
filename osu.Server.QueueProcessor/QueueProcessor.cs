using System;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;
using Newtonsoft.Json;
using osu.Framework.Threading;
using StackExchange.Redis;
using StatsdClient;

namespace osu.Server.QueueProcessor
{
    public abstract class QueueProcessor<T> where T : QueueItem
    {
        /// <summary>
        /// The total queue items processed since startup.
        /// </summary>
        public long TotalProcessed => totalProcessed;

        /// <summary>
        /// The total queue items dequeued since startup.
        /// </summary>
        public long TotalDequeued => totalDequeued;

        /// <summary>
        /// The total errors encountered processing items since startup.
        /// </summary>
        /// <remarks>
        /// Note that this may include more than one error from the same queue item failing multiple times.
        /// </remarks>
        public long TotalErrors => totalErrors;

        /// <summary>
        /// Report statistics about this queue via datadog.
        /// </summary>
        protected DogStatsdService DogStatsd { get; }

        private readonly QueueConfiguration config;

        /// <summary>
        /// An option queue to push to when finished.
        /// </summary>
        private readonly ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(
            Environment.GetEnvironmentVariable("REDIS_HOST") ?? "redis");

        private readonly string inputQueueName;

        private long totalProcessed;

        private long totalDequeued;

        private long totalErrors;

        private int consecutiveErrors;

        private long totalInFlight => totalDequeued - totalProcessed - totalErrors;

        protected QueueProcessor(QueueConfiguration config)
        {
            this.config = config;

            const string queue_prefix = "osu-queue:";

            inputQueueName = $"{queue_prefix}{config.InputQueueName}";

            DogStatsd = new DogStatsdService();
            DogStatsd.Configure(new StatsdConfig
            {
                StatsdServerName = Environment.GetEnvironmentVariable("DD_AGENT_HOST") ?? "localhost",
                Prefix = $"osu.server.queues.{config.InputQueueName}"
            });
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
                        if (consecutiveErrors > config.ErrorThreshold)
                            throw new Exception("Error threshold exceeded, shutting down");

                        T item;

                        try
                        {
                            if (totalInFlight > config.MaxInFlightItems || consecutiveErrors > config.ErrorThreshold)
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
                            DogStatsd.Increment("total_dequeued");

                            item = JsonConvert.DeserializeObject<T>(redisValue) ?? throw new InvalidOperationException("Dequeued item could not be deserialised.");

                            // individual processing should not be cancelled as we have already grabbed from the queue.
                            Task.Factory.StartNew(() => { ProcessResult(item); }, CancellationToken.None, TaskCreationOptions.HideScheduler, threadPool)
                                .ContinueWith(t =>
                                {
                                    if (t.Exception == null)
                                    {
                                        Interlocked.Increment(ref totalProcessed);

                                        // ReSharper disable once AccessToDisposedClosure
                                        DogStatsd.Increment("total_processed");

                                        Interlocked.Exchange(ref consecutiveErrors, 0);
                                    }
                                    else
                                    {
                                        Interlocked.Increment(ref totalErrors);

                                        // ReSharper disable once AccessToDisposedClosure
                                        DogStatsd.Increment("total_errors");

                                        Interlocked.Increment(ref consecutiveErrors);

                                        Console.WriteLine($"Error processing {item}: {t.Exception}");
                                        attemptRetry(item);
                                    }
                                }, CancellationToken.None);
                        }
                        catch (Exception e)
                        {
                            Interlocked.Increment(ref consecutiveErrors);
                            Console.WriteLine($"Error dequeueing from queue: {e}");
                        }
                    }

                    Console.WriteLine("Shutting down..");
                }
            }

            DogStatsd.Dispose();
            outputStats();
        }

        private void attemptRetry(T item)
        {
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
            try
            {
                DogStatsd.Gauge("in_flight", totalInFlight);
                Console.WriteLine($"stats: queue:{GetQueueSize()} inflight:{totalInFlight} dequeued:{totalDequeued} processed:{totalProcessed} errors:{totalErrors}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error outputting stats: {e}");
            }
        }

        public void PushToQueue(T obj) =>
            redis.GetDatabase().ListLeftPush(inputQueueName, JsonConvert.SerializeObject(obj));

        public long GetQueueSize() =>
            redis.GetDatabase().ListLength(inputQueueName);

        public void ClearQueue() => redis.GetDatabase().KeyDelete(inputQueueName);

        /// <summary>
        /// Retrieve a database connection.
        /// </summary>
        public virtual MySqlConnection GetDatabaseConnection()
        {
            string host = (Environment.GetEnvironmentVariable("DB_HOST") ?? "localhost");
            string user = (Environment.GetEnvironmentVariable("DB_USER") ?? "root");

            var connection = new MySqlConnection($"Server={host};Database=osu;User ID={user};ConnectionTimeout=5;ConnectionReset=false;Pooling=true;");
            connection.Open();

            // TODO: remove this when we have set a saner time zone server-side.
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SET time_zone = '+00:00';";
                cmd.ExecuteNonQuery();
            }

            return connection;
        }

        /// <summary>
        /// Implement to process a single item from the queue.
        /// </summary>
        /// <param name="item">The item to process.</param>
        protected abstract void ProcessResult(T item);
    }
}
