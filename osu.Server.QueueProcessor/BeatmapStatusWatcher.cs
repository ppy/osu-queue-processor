// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using MySqlConnector;

namespace osu.Server.QueueProcessor
{
    /// <summary>
    /// Provides insight into whenever a beatmap has changed status based on a user or system update.
    /// </summary>
    public static class BeatmapStatusWatcher
    {
        /// <summary>
        /// Start a background task which will poll for beatmap sets with updates.
        /// </summary>
        /// <remarks>
        /// Prior to polling, a blocking call to <see cref="GetUpdatedBeatmapSets"/> is required to ensure no initial updates are missed.
        /// The general flow of usage should be:
        ///
        /// // before doing anything else
        /// var updates = await GetUpdatedBeatmapSets();
        /// // can now query and cache beatmaps.
        /// StartPolling(updates, callback);
        ///
        /// void callback(BeatmapUpdates u)
        /// {
        ///     foreach (int id in u.BeatmapSetIDs)
        ///     {
        ///         // invalidate `id`
        ///     }
        /// }
        /// </remarks>
        /// <param name="initialUpdates">The response from an initial call to <see cref="GetUpdatedBeatmapSets"/>.</param>
        /// <param name="callback">A callback to receive information about any updated beatmap sets.</param>
        /// <param name="pollMilliseconds">The number of milliseconds to wait between polls. Starts counting from response of previous poll.</param>
        /// <param name="limit">The maximum number of beatmap sets to return in a single response.</param>
        /// <returns>An <see cref="IDisposable"/> that should be disposed of to stop polling.</returns>
        public static IDisposable StartPolling(BeatmapUpdates initialUpdates, Action<BeatmapUpdates> callback, int pollMilliseconds = 10000, int limit = 50) =>
            new PollingBeatmapStatusWatcher(callback, pollMilliseconds, limit);

        /// <summary>
        /// Check for any beatmap sets with updates since the provided queue ID.
        /// Should be called on a regular basis. See <see cref="StartPolling"/> for automatic polling after the first call.
        /// </summary>
        /// <param name="lastQueueId">The last checked queue ID, ie <see cref="BeatmapUpdates.LastProcessedQueueID"/>.</param>
        /// <param name="limit">The maximum number of beatmap sets to return in a single response.</param>
        /// <returns>A response containing information about any updated beatmap sets.</returns>
        public static async Task<BeatmapUpdates> GetUpdatedBeatmapSets(int? lastQueueId, int limit = 50)
        {
            MySqlConnection connection = await DatabaseAccess.GetConnectionAsync();

            if (lastQueueId.HasValue)
            {
                var items = (await connection.QueryAsync<bss_process_queue_item>("SELECT * FROM bss_process_queue WHERE queue_id > @lastQueueId LIMIT @limit", new
                {
                    lastQueueId,
                    limit
                })).ToArray();

                return new BeatmapUpdates
                {
                    BeatmapSetIDs = items.Select(i => i.beatmapset_id).ToArray(),
                    LastProcessedQueueID = items.LastOrDefault()?.queue_id ?? lastQueueId.Value
                };
            }

            var lastEntry = await connection.QueryFirstOrDefaultAsync<bss_process_queue_item>("SELECT * FROM bss_process_queue ORDER BY queue_id DESC LIMIT 1");

            return new BeatmapUpdates
            {
                BeatmapSetIDs = [],
                LastProcessedQueueID = lastEntry?.queue_id ?? 0
            };
        }

        // ReSharper disable InconsistentNaming (matches database table)
        [Serializable]
        public class bss_process_queue_item
        {
            public int queue_id;
            public int beatmapset_id;
        }

        public record BeatmapUpdates
        {
            public required int[] BeatmapSetIDs { get; init; }
            public required int LastProcessedQueueID { get; init; }
        }
    }

    public class PollingBeatmapStatusWatcher : IDisposable
    {
        private readonly Action<BeatmapStatusWatcher.BeatmapUpdates> callback;

        private readonly int pollMilliseconds;
        private readonly int limit;

        private int? lastQueueId;
        private readonly CancellationTokenSource cts;

        public PollingBeatmapStatusWatcher(Action<BeatmapStatusWatcher.BeatmapUpdates> callback, int pollMilliseconds, int limit = 50)
        {
            this.pollMilliseconds = pollMilliseconds;
            this.limit = limit;
            this.callback = callback;

            cts = new CancellationTokenSource();

            _ = poll();
        }

        private async Task poll()
        {
            try
            {
                var result = await BeatmapStatusWatcher.GetUpdatedBeatmapSets(lastQueueId, limit);

                lastQueueId = result.LastProcessedQueueID;
                if (result.BeatmapSetIDs.Length > 0)
                    callback(result);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Poll failed with {e}.");
                await Task.Delay(1000);
            }

            _ = Task.Delay(pollMilliseconds, cts.Token).ContinueWith(_ => poll(), cts.Token);
        }

        public void Dispose()
        {
            cts.Cancel();
            cts.Dispose();
        }
    }
}
