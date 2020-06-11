using System;

namespace osu.Server.QueueProcessor.Tests
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