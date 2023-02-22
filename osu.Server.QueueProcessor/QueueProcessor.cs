using System;
using System.Collections.Generic;
using System.Linq;
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

        public event Action<Exception?, T>? Error;

        /// <summary>
        /// The name of this queue, as provided by <see cref="QueueConfiguration"/>.
        /// </summary>
        public string QueueName { get; }

        /// <summary>
        /// Report statistics about this queue via datadog.
        /// </summary>
        protected DogStatsdService DogStatsd { get; }

        private readonly QueueConfiguration config;

        /// <summary>
        /// An option queue to push to when finished.
        /// </summary>
        private readonly ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(
            Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost");

        private long totalProcessed;

        private long totalDequeued;

        private long totalErrors;

        private int consecutiveErrors;

        private long totalInFlight => totalDequeued - totalProcessed - totalErrors;

        protected QueueProcessor(QueueConfiguration config)
        {
            this.config = config;

            const string queue_prefix = "osu-queue:";

            QueueName = $"{queue_prefix}{config.InputQueueName}";

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

                        try
                        {
                            if (totalInFlight >= config.MaxInFlightItems || consecutiveErrors > config.ErrorThreshold)
                            {
                                Thread.Sleep(config.TimeBetweenPolls);
                                continue;
                            }

                            var redisItems = database.ListRightPop(QueueName, config.BatchSize);

                            // queue doesn't exist.
                            if (redisItems == null)
                            {
                                Thread.Sleep(config.TimeBetweenPolls);
                                continue;
                            }

                            List<T> items = new List<T>();

                            // null or empty check is required for redis 6.x. 7.x reports `null` instead.
                            foreach (var redisItem in redisItems.Where(i => !i.IsNullOrEmpty))
                                items.Add(JsonConvert.DeserializeObject<T>(redisItem, config.JsonSerializerSettings) ?? throw new InvalidOperationException("Dequeued item could not be deserialised."));

                            if (items.Count == 0)
                            {
                                Thread.Sleep(config.TimeBetweenPolls);
                                continue;
                            }

                            Interlocked.Add(ref totalDequeued, items.Count);
                            DogStatsd.Increment("total_dequeued", items.Count);

                            // individual processing should not be cancelled as we have already grabbed from the queue.
                            Task.Factory.StartNew(() => { ProcessResults(items); }, CancellationToken.None, TaskCreationOptions.HideScheduler, threadPool)
                                .ContinueWith(t =>
                                {
                                    foreach (var item in items)
                                    {
                                        if (t.Exception != null || item.Failed)
                                        {
                                            Interlocked.Increment(ref totalErrors);

                                            // ReSharper disable once AccessToDisposedClosure
                                            DogStatsd.Increment("total_errors", tags: item.Tags);

                                            Interlocked.Increment(ref consecutiveErrors);

                                            Error?.Invoke(t.Exception, item);

                                            Console.WriteLine($"Error processing {item}: {t.Exception}");
                                            attemptRetry(item);
                                        }
                                        else
                                        {
                                            Interlocked.Increment(ref totalProcessed);

                                            // ReSharper disable once AccessToDisposedClosure
                                            DogStatsd.Increment("total_processed", tags: item.Tags);

                                            Interlocked.Exchange(ref consecutiveErrors, 0);
                                        }
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

                    while (totalInFlight > 0)
                    {
                        Console.WriteLine($"Waiting for remaining {totalInFlight} in-flight items...");
                        Thread.Sleep(5000);
                    }

                    Console.WriteLine("Bye!");
                }
            }

            DogStatsd.Dispose();
            outputStats();
        }

        private void attemptRetry(T item)
        {
            item.Failed = false;

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

        /// <summary>
        /// Push a single item to the queue.
        /// </summary>
        /// <param name="item"></param>
        public void PushToQueue(T item) =>
            redis.GetDatabase().ListLeftPush(QueueName, JsonConvert.SerializeObject(item));

        /// <summary>
        /// Push multiple items to the queue.
        /// </summary>
        /// <param name="items"></param>
        public void PushToQueue(IEnumerable<T> items) =>
            redis.GetDatabase().ListLeftPush(QueueName, items.Select(obj => new RedisValue(JsonConvert.SerializeObject(obj))).ToArray());

        public long GetQueueSize() =>
            redis.GetDatabase().ListLength(QueueName);

        public void ClearQueue() => redis.GetDatabase().KeyDelete(QueueName);

        /// <summary>
        /// Publishes a message to a Redis channel with the supplied <paramref name="channelName"/>.
        /// </summary>
        /// <remarks>
        /// The message will be serialised using JSON.
        /// Successful publications are tracked in Datadog, using the <paramref name="channelName"/> and the <typeparamref name="TMessage"/>'s full type name as a tag.
        /// </remarks>
        /// <param name="channelName">The name of the Redis channel to publish to.</param>
        /// <param name="message">The message to publish to the channel.</param>
        /// <typeparam name="TMessage">The type of message to be published.</typeparam>
        public void PublishMessage<TMessage>(string channelName, TMessage message)
        {
            redis.GetDatabase().Publish(channelName, JsonConvert.SerializeObject(message));
            DogStatsd.Increment("messages_published", tags: new[] { $"channel:{channelName}", $"type:{typeof(TMessage).FullName}" });
        }

        /// <summary>
        /// Retrieve a database connection.
        /// </summary>
        public virtual MySqlConnection GetDatabaseConnection()
        {
            string connectionString = Environment.GetEnvironmentVariable("DB_CONNECTION_STRING") ?? String.Empty;

            if (string.IsNullOrEmpty(connectionString))
            {
                string host = (Environment.GetEnvironmentVariable("DB_HOST") ?? "localhost");
                string user = (Environment.GetEnvironmentVariable("DB_USER") ?? "root");
                string password = (Environment.GetEnvironmentVariable("DB_PASS") ?? string.Empty);
                string name = (Environment.GetEnvironmentVariable("DB_NAME") ?? "osu");

                string passwordString = string.IsNullOrEmpty(password) ? string.Empty : $"Password={password};";

                connectionString = $"Server={host};Database={name};User ID={user};{passwordString}ConnectionTimeout=5;ConnectionReset=false;Pooling=true;";
            }

            var connection = new MySqlConnection(connectionString);
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
        /// Implement to process a single item from the queue. Will only be invoked if <see cref="ProcessResults"/> is not implemented.
        /// </summary>
        /// <param name="item">The item to process.</param>
        protected virtual void ProcessResult(T item)
        {
        }

        /// <summary>
        /// Implement to process batches of items from the queue.
        /// </summary>
        /// <param name="items">The items to process.</param>
        protected virtual void ProcessResults(IEnumerable<T> items)
        {
            foreach (var item in items)
                ProcessResult(item);
        }
    }
}
