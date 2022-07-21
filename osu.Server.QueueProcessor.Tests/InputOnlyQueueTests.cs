using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Assert = Xunit.Assert;

namespace osu.Server.QueueProcessor.Tests
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

            var obj = FakeData.New();

            FakeData? receivedObject = null;

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

            var objects = new HashSet<FakeData>();
            for (int i = 0; i < send_count; i++)
                objects.Add(FakeData.New());

            var receivedObjects = new HashSet<FakeData>();

            foreach (var obj in objects)
                processor.PushToQueue(obj);

            processor.Received += o =>
            {
                lock (receivedObjects)
                    receivedObjects.Add(o);

                if (receivedObjects.Count == send_count)
                    cts.Cancel();
            };

            processor.Run(cts.Token);

            Assert.Equal(objects, receivedObjects);
        }

        /// <summary>
        /// If the processor is cancelled mid-operation, every item should either be processed or still in the queue.
        /// </summary>
        [Fact]
        public void EnsureCancellingDoesNotLoseItems()
        {
            var inFlightObjects = new List<FakeData>();

            int processed = 0;
            int sent = 0;

            processor.Received += o =>
            {
                lock (inFlightObjects)
                {
                    inFlightObjects.Remove(o);
                    Interlocked.Increment(ref processed);
                }
            };

            const int run_count = 5;

            // start and stop processing multiple times, checking items are in a good state each time.

            for (int i = 0; i < run_count; i++)
            {
                var cts = new CancellationTokenSource();

                var sendTask = Task.Run(() =>
                {
                    while (!cts.IsCancellationRequested)
                    {
                        var obj = FakeData.New();

                        lock (inFlightObjects)
                        {
                            processor.PushToQueue(obj);
                            inFlightObjects.Add(obj);
                        }

                        Interlocked.Increment(ref sent);
                    }
                }, CancellationToken.None);

                // Ensure there are some items in the queue before starting the processor.
                while (inFlightObjects.Count < 1000)
                    Thread.Sleep(100);

                var receiveTask = Task.Run(() => processor.Run(cts.Token), CancellationToken.None);

                Thread.Sleep(1000);

                cts.Cancel();

                sendTask.Wait(10000);
                receiveTask.Wait(10000);

                output.WriteLine($"Sent: {sent} In-flight: {inFlightObjects.Count} Processed: {processed}");
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

            output.WriteLine($"Sent: {sent} In-flight: {inFlightObjects.Count} Processed: {processed}");
        }

        [Fact]
        public void SendThenErrorDoesRetry()
        {
            var cts = new CancellationTokenSource(10000);

            var obj = FakeData.New();

            FakeData? receivedObject = null;

            bool didThrowOnce = false;

            processor.PushToQueue(obj);

            processor.Received += o =>
            {
                if (o.TotalRetries == 0)
                {
                    didThrowOnce = true;
                    throw new Exception();
                }

                receivedObject = o;
                cts.Cancel();
            };

            processor.Run(cts.Token);

            Assert.True(didThrowOnce);
            Assert.Equal(obj, receivedObject);
        }

        [Fact]
        public void SendThenErrorForeverDoesDrop()
        {
            var cts = new CancellationTokenSource(10000);

            var obj = FakeData.New();

            int attemptCount = 0;

            processor.PushToQueue(obj);

            processor.Received += o =>
            {
                attemptCount++;
                if (attemptCount > 3)
                    cts.Cancel();

                throw new Exception();
            };

            processor.Run(cts.Token);

            Assert.Equal(4, attemptCount);
            Assert.Equal(0, processor.GetQueueSize());
        }

        [Fact]
        public void ExitOnErrorThresholdHit()
        {
            var cts = new CancellationTokenSource(10000);

            int attemptCount = 0;

            // 3 retries for each, so at least one should remain in queue.
            processor.PushToQueue(FakeData.New());
            processor.PushToQueue(FakeData.New());
            processor.PushToQueue(FakeData.New());
            processor.PushToQueue(FakeData.New());

            processor.Received += o =>
            {
                attemptCount++;
                throw new Exception();
            };

            Assert.Throws<Exception>(() => processor.Run(cts.Token));

            Assert.True(attemptCount >= 10, "attemptCount >= 10");
            Assert.NotEqual(0, processor.GetQueueSize());
        }
    }
}
