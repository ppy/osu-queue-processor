using System;
using System.Threading;
using System.Threading.Tasks;
using OsuQueueProcessor;

namespace TestQueueProcessor
{
    static class Program
    {
        static void Main()
        {
            var processor = new TestProcessor();

            Task.Run(() =>
            {
                while (true)
                {
                    var obj = new TestData(Guid.NewGuid());
                    processor.PushToQueue(obj);
                    Thread.Sleep(20);
                }

                // ReSharper disable once FunctionNeverReturns
            });

            Task.Run(() => { processor.Run(); });

            Console.ReadLine();
        }
    }

    [Serializable]
    public class TestData
    {
        public readonly Guid Data;

        public TestData(Guid data)
        {
            this.Data = data;
        }

        public override string ToString() => Data.ToString();
    }

    public class TestProcessor : QueueProcessor<TestData>
    {
        public TestProcessor()
            : base(new QueueConfiguration
            {
                InputQueueName = "test"
            })
        {
        }

        protected override void ProcessResult(TestData result)
        {
            Console.WriteLine($"Got result: {result}");
        }
    }
}