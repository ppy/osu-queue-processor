using System;
using System.Threading;
using OsuQueueProcessor;
using Xunit;

namespace QueueProcessorTests
{
    public class BasicQueueTests
    {
        [Fact]
        public void SendThenReceive_Single()
        {
            var cts = new CancellationTokenSource();

            var processor = new TestProcessor();

            var obj = new FakeData();
            FakeData receivedObject = null;

            processor.PushToQueue(obj);

            processor.Received += o =>
            {
                receivedObject = o;
                cts.Cancel();
            };

            processor.Run(cts.Token);

            cts.Token.WaitHandle.WaitOne(10000);
            
            Assert.Equal(obj, receivedObject);
        }
    }

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