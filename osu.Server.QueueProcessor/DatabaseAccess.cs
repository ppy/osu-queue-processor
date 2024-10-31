// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

using System;
using MySqlConnector;

namespace osu.Server.QueueProcessor
{
    /// <summary>
    /// Provides access to a MySQL database.
    /// </summary>
    public static class DatabaseAccess
    {
        /// <summary>
        /// Retrieve a fresh MySQL connection. Should be disposed after use.
        /// </summary>
        public static MySqlConnection GetConnection()
        {
            string connectionString = Environment.GetEnvironmentVariable("DB_CONNECTION_STRING") ?? String.Empty;

            if (string.IsNullOrEmpty(connectionString))
            {
                string host = (Environment.GetEnvironmentVariable("DB_HOST") ?? "localhost");
                string port = (Environment.GetEnvironmentVariable("DB_PORT") ?? "3306");
                string user = (Environment.GetEnvironmentVariable("DB_USER") ?? "root");
                string password = (Environment.GetEnvironmentVariable("DB_PASS") ?? string.Empty);
                string name = (Environment.GetEnvironmentVariable("DB_NAME") ?? "osu");
                bool pooling = Environment.GetEnvironmentVariable("DB_POOLING") != Boolean.FalseString && Environment.GetEnvironmentVariable("DB_POOLING") != "0";
                int maxPoolSize = int.Parse(Environment.GetEnvironmentVariable("DB_MAX_POOL_SIZE") ?? "100");

                string passwordString = string.IsNullOrEmpty(password) ? string.Empty : $"Password={password};";

                connectionString = $"Server={host};Port={port};Database={name};User ID={user};{passwordString}ConnectionTimeout=5;ConnectionReset=false;Pooling={pooling};Max Pool Size={maxPoolSize};";
            }

            var connection = new MySqlConnection(connectionString);
            connection.Open();

            // TODO: remove this when we have set a saner time zone server-side.
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SET time_zone = '+00:00';";
                cmd.ExecuteNonQuery();
            }

            return connection;
        }
    }
}
