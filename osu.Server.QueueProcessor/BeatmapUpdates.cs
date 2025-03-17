// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

namespace osu.Server.QueueProcessor
{
    public record BeatmapUpdates
    {
        public required int[] BeatmapSetIDs { get; init; }
        public required int LastProcessedQueueID { get; init; }
    }
}
