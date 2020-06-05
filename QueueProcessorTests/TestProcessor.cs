using System;
using osu.Server.QueueProcessor;

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

        protected override void ProcessResult(FakeData result) => Received?.Invoke(result);

        public Action<FakeData> Received;
    }
}