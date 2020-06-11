// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

using System;

namespace osu.Server.QueueProcessor
{
    /// <summary>
    /// An item to be managed by a <see cref="QueueProcessor{T}"/>.
    /// </summary>
    [Serializable]
    public abstract class QueueItem
    {
        public int TotalRetries { get; set; }
    }
}
