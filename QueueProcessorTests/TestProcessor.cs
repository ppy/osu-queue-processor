using System;
using OsuQueueProcessor;

namespace QueueProcessorTests
{
    public class TestProcessor : QueueProcessor<FakeData>
    {
        public TestProcessor()
            : base(new QueueConfiguration
            {
                InputQueueName = "test"
            })
        {
        }

        protected override void ProcessResult(FakeData result)
        {
            Console.WriteLine($"Got result: {result}");
            Received?.Invoke(result);
        }

        public Action<FakeData> Received;
    }
}