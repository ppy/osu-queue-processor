// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

using System;
using System.Runtime.Serialization;

namespace osu.Server.QueueProcessor
{
    /// <summary>
    /// An item to be managed by a <see cref="QueueProcessor{T}"/>.
    /// </summary>
    [Serializable]
    public abstract class QueueItem
    {
        /// <summary>
        /// Set to <c>true</c> to mark this item is failed. This will cause it to be retried.
        /// </summary>
        [IgnoreDataMember]
        public bool Failed { get; set; }

        /// <summary>
        /// The number of times processing this item has been retried. Handled internally by <see cref="QueueProcessor{T}"/>.
        /// </summary>
        public int TotalRetries { get; set; }

        /// <summary>
        /// Tags which will be used for tracking a processed item.
        /// </summary>
        public string[]? Tags { get; set; }
    }
}
