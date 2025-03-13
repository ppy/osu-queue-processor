// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

using System;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Xunit;

namespace osu.Server.QueueProcessor.Tests
{
    public class BeatmapStatusWatcherTests
    {
        /// <summary>
        /// Checking that processing an empty queue works as expected.
        /// </summary>
        [Fact]
        public async Task TestBasic()
        {
            var cts = new CancellationTokenSource(10000);

            TaskCompletionSource<BeatmapUpdates> tcs = new TaskCompletionSource<BeatmapUpdates>();
            using var db = await DatabaseAccess.GetConnectionAsync(cts.Token);

            // just a safety measure for now to ensure we don't hit production. since i was running on production until now.
            // will throw if not on test database.
            if (db.QueryFirstOrDefault<int?>("SELECT `count` FROM `osu_counts` WHERE `name` = 'is_production'") != null)
                throw new InvalidOperationException("You are trying to do something very silly.");

            await db.ExecuteAsync("TRUNCATE TABLE `bss_process_queue`");

            using var poller = await BeatmapStatusWatcher.StartPollingAsync(updates => { tcs.SetResult(updates); }, pollMilliseconds: 100);

            await db.ExecuteAsync("INSERT INTO `bss_process_queue` (beatmapset_id) VALUES (1)");

            var updates = await tcs.Task.WaitAsync(cts.Token);

            Assert.Equal(new[] { 1 }, updates.BeatmapSetIDs);
            Assert.Equal(1, updates.LastProcessedQueueID);

            tcs = new TaskCompletionSource<BeatmapUpdates>();

            await db.ExecuteAsync("INSERT INTO `bss_process_queue` (beatmapset_id) VALUES (2), (3)");

            updates = await tcs.Task.WaitAsync(cts.Token);

            Assert.Equal(new[] { 2, 3 }, updates.BeatmapSetIDs);
            Assert.Equal(3, updates.LastProcessedQueueID);
        }

        /// <summary>
        /// Checking that processing an empty queue works as expected.
        /// </summary>
        [Fact]
        public async Task TestLimit()
        {
            var cts = new CancellationTokenSource(10000);

            TaskCompletionSource<BeatmapUpdates> tcs = new TaskCompletionSource<BeatmapUpdates>();
            using var db = await DatabaseAccess.GetConnectionAsync(cts.Token);

            // just a safety measure for now to ensure we don't hit production. since i was running on production until now.
            // will throw if not on test database.
            if (db.QueryFirstOrDefault<int?>("SELECT `count` FROM `osu_counts` WHERE `name` = 'is_production'") != null)
                throw new InvalidOperationException("You are trying to do something very silly.");

            await db.ExecuteAsync("TRUNCATE TABLE `bss_process_queue`");

            using var poller = await BeatmapStatusWatcher.StartPollingAsync(updates => { tcs.SetResult(updates); }, limit: 1, pollMilliseconds: 100);

            await db.ExecuteAsync("INSERT INTO `bss_process_queue` (beatmapset_id) VALUES (1)");

            var updates = await tcs.Task.WaitAsync(cts.Token);
            tcs = new TaskCompletionSource<BeatmapUpdates>();

            Assert.Equal(new[] { 1 }, updates.BeatmapSetIDs);
            Assert.Equal(1, updates.LastProcessedQueueID);

            await db.ExecuteAsync("INSERT INTO `bss_process_queue` (beatmapset_id) VALUES (2), (3)");

            updates = await tcs.Task.WaitAsync(cts.Token);
            tcs = new TaskCompletionSource<BeatmapUpdates>();

            Assert.Equal(new[] { 2 }, updates.BeatmapSetIDs);
            Assert.Equal(2, updates.LastProcessedQueueID);

            updates = await tcs.Task.WaitAsync(cts.Token);

            Assert.Equal(new[] { 3 }, updates.BeatmapSetIDs);
            Assert.Equal(3, updates.LastProcessedQueueID);
        }
    }
}
