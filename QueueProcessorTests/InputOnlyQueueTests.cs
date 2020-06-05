using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Xunit;
using Xunit.Abstractions;
using Assert = Xunit.Assert;

namespace QueueProcessorTests
{
    public class InputOnlyQueueTests
    {
        private readonly ITestOutputHelper output;
        private readonly TestProcessor processor;

        public InputOnlyQueueTests(ITestOutputHelper output)
        {
            this.output = output;

            processor = new TestProcessor();
            processor.ClearQueue();
        }

        [Fact]
        public void SendThenReceive_Single()
        {
            var cts = new CancellationTokenSource(10000);

            var obj = new FakeData();
            FakeData receivedObject = null;

            processor.PushToQueue(obj);

            processor.Received += o =>
            {
                receivedObject = o;
                cts.Cancel();
            };

            processor.Run(cts.Token);

            Assert.Equal(obj, receivedObject);
        }

        [Fact]
        public void SendThenReceive_Multiple()
        {
            const int send_count = 20;

            var cts = new CancellationTokenSource(10000);

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

            processor.Run(cts.Token);

            CollectionAssert.AreEquivalent(objects, receivedObjects);
        }

        /// <summary>
        /// If the processor is cancelled mid-operation, every item should either be processed or still in the queue.
        /// </summary>
        [Fact]
        public void EnsureCancellingDoesNotLoseItems()
        {
            var inFlightObjects = new List<FakeData>();

            processor.Received += o =>
            {
                lock (inFlightObjects)
                    inFlightObjects.Remove(o);
            };

            // start and stop processing multiple times, checking items are in a good state each time.
            for (int i = 0; i < 10; i++)
            {
                var cts = new CancellationTokenSource();

                var sendTask = Task.Run(() =>
                {
                    while (!cts.IsCancellationRequested)
                    {
                        var obj = new FakeData();

                        processor.PushToQueue(obj);
                        lock (inFlightObjects)
                            inFlightObjects.Add(obj);
                    }
                }, cts.Token);

                var receiveTask = Task.Run(() => processor.Run((cts = new CancellationTokenSource()).Token), cts.Token);

                Thread.Sleep(500);

                cts.Cancel();

                sendTask.Wait(10000);
                receiveTask.Wait(10000);

                output.WriteLine($"In-flight objects: {inFlightObjects.Count}");

                Assert.Equal(inFlightObjects.Count, processor.GetQueueSize());
            }

            var finalCts = new CancellationTokenSource(10000);

            processor.Received += _ =>
            {
                if (inFlightObjects.Count == 0)
                    // early cancel once the list is emptied.
                    finalCts.Cancel();
            };

            // process all remaining items
            processor.Run(finalCts.Token);

            Assert.Empty(inFlightObjects);
            Assert.Equal(0, processor.GetQueueSize());
        }
    }
}