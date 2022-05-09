using System;
using System.Runtime.Loader;
using System.Threading;

namespace osu.Server.QueueProcessor
{
    public class GracefulShutdownSource : IDisposable
    {
        public CancellationToken Token => cts.Token;

        private readonly ManualResetEventSlim shutdownComplete;

        private readonly CancellationTokenSource cts;

        public void Cancel() => cts.Cancel();

        public GracefulShutdownSource(in CancellationToken cancellation = default)
        {
            cts = CancellationTokenSource.CreateLinkedTokenSource(cancellation);

            shutdownComplete = new ManualResetEventSlim();

            AssemblyLoadContext.Default.Unloading += onUnloading;
            Console.CancelKeyPress += onCancelKeyPress;
        }

        private void onCancelKeyPress(object? sender, ConsoleCancelEventArgs args)
        {
            args.Cancel = true;
            cts.Cancel();
        }

        private void onUnloading(AssemblyLoadContext _)
        {
            cts.Cancel();
            shutdownComplete.Wait(CancellationToken.None);
        }

        public void Dispose()
        {
            shutdownComplete.Set();
            Console.CancelKeyPress -= onCancelKeyPress;
        }
    }
}
