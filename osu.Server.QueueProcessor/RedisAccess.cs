// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

using System;
using StackExchange.Redis;

namespace osu.Server.QueueProcessor
{
    /// <summary>
    /// Provides access to a Redis database.
    /// </summary>
    public static class RedisAccess
    {
        private static readonly ConfigurationOptions redis_config = ConfigurationOptions.Parse(Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost");

        /// <summary>
        /// Retrieve a fresh Redis connection. Should be disposed after use.
        /// </summary>
        public static ConnectionMultiplexer GetConnection() => ConnectionMultiplexer.Connect(redis_config);
    }
}
