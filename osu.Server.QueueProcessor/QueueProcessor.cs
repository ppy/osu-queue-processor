using System;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using osu.Framework.Logging;
using osu.Framework.Threading;
using StackExchange.Redis;

namespace osu.Server.QueueProcessor
{
    public abstract class QueueProcessor<T>
    {
        private readonly QueueConfiguration config;

        /// <summary>
        /// An option queue to push to when finished.
        /// </summary>
        private readonly ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(
            Environment.GetEnvironmentVariable("REDIS_HOST") ?? "redis");

        private readonly string inputQueueName;

        protected QueueProcessor(QueueConfiguration config)
        {
            this.config = config;

            const string queue_prefix = "osu-queue:";

            inputQueueName = $"{queue_prefix}{config.InputQueueName}";
        }

        /// <summary>
        /// Start running the queue.
        /// </summary>
        /// <param name="cancellation"></param>
        public void Run(CancellationToken cancellation)
        {
            Logger.Log("Starting queue processing..");

            using var threadPool = new ThreadedTaskScheduler(Environment.ProcessorCount, "workers");

            var database = redis.GetDatabase();

            while (!cancellation.IsCancellationRequested)
            {
                try
                {
                    var redisValue = database.ListRightPop(inputQueueName);

                    if (!redisValue.HasValue)
                    {
                        Thread.Sleep(config.TimeBetweenPolls);
                        continue;
                    }

                    var item = JsonConvert.DeserializeObject<T>(redisValue);

                    // individual processing should not be cancelled as we have already grabbed from the queue.
                    Task.Factory.StartNew(() => ProcessResult(item), default, TaskCreationOptions.HideScheduler, threadPool)
                        .ContinueWith(t =>
                        {
                            if (t.Exception != null)
                                Logger.Error(t.Exception, $"Error processing {item}");
                        }, CancellationToken.None);
                }
                catch (Exception e)
                {
                    Logger.Error(e, $"Error processing from queue");
                }
            }
        }

        public void PushToQueue(T obj) =>
            redis.GetDatabase().ListLeftPush(inputQueueName, JsonConvert.SerializeObject(obj));

        public long GetQueueSize() =>
            redis.GetDatabase().ListLength(inputQueueName);

        public void ClearQueue() => redis.GetDatabase().KeyDelete(inputQueueName);

        /// <summary>
        /// Implement to process a single item from the queue.
        /// </summary>
        /// <param name="item">The item to process.</param>
        protected abstract void ProcessResult(T item);
    }
}