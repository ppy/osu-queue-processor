namespace OsuQueueProcessor
{
    public class QueueConfiguration
    {
        /// <summary>
        /// The queue to read from.
        /// </summary>
        public string InputQueueName { get; set; } = "default";

        public int TimeBetweenPolls { get; set; } = 100;
    }
}