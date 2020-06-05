using System;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using osu.Framework.Logging;
using osu.Framework.Threading;
using StackExchange.Redis;

namespace OsuQueueProcessor
{
    public abstract class QueueProcessor<T>
    {
        private readonly QueueConfiguration config;

        /// <summary>
        /// An option queue to push to when finished.
        /// </summary>
        private readonly ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("redis");

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
        public void Run()
        {
            Logger.Log("Starting queue processing..");

            var threadPool = new ThreadedTaskScheduler(Environment.ProcessorCount, "workers");

            var database = redis.GetDatabase();

            while (true)
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

                    Task.Factory.StartNew(() => ProcessResult(item), default, TaskCreationOptions.HideScheduler, threadPool)
                        .ContinueWith(t =>
                        {
                            if (t.Exception != null)
                                Logger.Error(t.Exception, $"Error processing {item}");
                        });
                }
                catch (Exception e)
                {
                    Logger.Error(e, $"Error processing from queue");
                }
            }

            // ReSharper disable once FunctionNeverReturns
        }

        public void PushToQueue(T obj)
        {
            Logger.Log($"Pushing {obj} to queue...");
            redis.GetDatabase().ListLeftPush(inputQueueName, JsonConvert.SerializeObject(obj));
        }

        /// <summary>
        /// Implement to process a single item from the queue.
        /// </summary>
        /// <param name="item">The item to process.</param>
        protected abstract void ProcessResult(T item);
    }
}