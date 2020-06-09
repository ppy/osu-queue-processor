namespace osu.Server.QueueProcessor
{
    public class QueueConfiguration
    {
        /// <summary>
        /// The queue to read from.
        /// </summary>
        public string InputQueueName { get; set; } = "default";

        public int TimeBetweenPolls { get; set; } = 100;

        /// <summary>
        /// The number of items allowed to be dequeued but not processed at one time.
        /// </summary>
        public int MaxInFlightItems { get; set; } = 100;
    }
}