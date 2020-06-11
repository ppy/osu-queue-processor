namespace osu.Server.QueueProcessor
{
    public class QueueConfiguration
    {
        /// <summary>
        /// The queue to read from.
        /// </summary>
        public string InputQueueName { get; set; } = "default";

        /// <summary>
        /// The time between polls (in the case a poll returns no items).
        /// </summary>
        public int TimeBetweenPolls { get; set; } = 100;

        /// <summary>
        /// The number of items allowed to be dequeued but not processed at one time.
        /// </summary>
        public int MaxInFlightItems { get; set; } = 100;

        /// <summary>
        /// The number of times to re-queue a failed item for another attempt.
        /// </summary>
        public int MaxRetries { get; set; } = 3;

        /// <summary>
        /// The maximum number of recent errors before exiting with an error.
        /// </summary>
        /// <remarks>
        /// Every error will increment an internal count, while every success will decrement it.
        /// </remarks>
        public int ErrorThreshold { get; set; } = 10;
    }
}