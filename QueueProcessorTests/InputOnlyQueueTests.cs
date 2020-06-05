using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Xunit;
using Assert = Xunit.Assert;

namespace QueueProcessorTests
{
    public class InputOnlyQueueTests
    {
        private readonly TestProcessor processor;

        public InputOnlyQueueTests()
        {
            processor = new TestProcessor();
            processor.ClearQueue();
        }

        [Fact]
        public void SendThenReceive_Single()
        {
            var cts = new CancellationTokenSource();


            var obj = new FakeData();
            FakeData receivedObject = null;

            processor.PushToQueue(obj);

            processor.Received += o =>
            {
                receivedObject = o;
                cts.Cancel();
            };

            Task.Run(() => processor.Run(cts.Token), cts.Token);

            cts.Token.WaitHandle.WaitOne(10000);

            Assert.Equal(obj, receivedObject);
        }

        [Fact]
        public void SendThenReceive_Multiple()
        {
            const int send_count = 20;

            var cts = new CancellationTokenSource();

            var objects = new List<FakeData>();
            for (int i = 0; i < send_count; i++)
                objects.Add(new FakeData());

            List<FakeData> receivedObjects = new List<FakeData>();

            foreach (var obj in objects)
                processor.PushToQueue(obj);

            processor.Received += o =>
            {
                receivedObjects.Add(o);

                if (receivedObjects.Count == send_count)
                    cts.Cancel();
            };

            Task.Run(() => processor.Run(cts.Token), cts.Token);

            cts.Token.WaitHandle.WaitOne(10000);

            CollectionAssert.AreEquivalent(objects, receivedObjects);
        }
    }
}