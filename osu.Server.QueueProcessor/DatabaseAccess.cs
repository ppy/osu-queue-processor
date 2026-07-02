// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

using System;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;
using Microsoft.Extensions.Logging;

namespace osu.Server.QueueProcessor
{
    /// <summary>
    /// Provides access to a MySQL database.
    /// </summary>
    public static class DatabaseAccess
    {
        private static readonly MySqlDataSource data_source;

        static DatabaseAccess()
        {
            var sourceBuilder = new MySqlDataSourceBuilder(getConnectionString());

            if (Environment.GetEnvironmentVariable("DB_LOGGING") == "1")
            {
                sourceBuilder.UseLoggerFactory(LoggerFactory.Create(builder =>
                {
                    builder
                        .AddSimpleConsole(options => options.TimestampFormat = "HH:mm:ss ")
                        .SetMinimumLevel(LogLevel.Trace)
                        .AddFilter("MySqlConnector", LogLevel.Trace);
                }));
            }

            data_source = sourceBuilder.Build();
        }

        public static MySqlConnection GetConnection()
        {
            MySqlConnection connection = data_source.CreateConnection();

            connection.Open();

            using (MySqlCommand command = connection.CreateCommand())
            {
                command.CommandText = "SET time_zone = '+00:00';";
                command.ExecuteNonQuery();
            }

            return connection;
        }

        /// <summary>
        /// Retrieve a fresh MySQL connection. Should be disposed after use.
        /// </summary>
        public static async Task<MySqlConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
        {
            var connection = new MySqlConnection(getConnectionString());

            await connection.OpenAsync(cancellationToken);

            // TODO: remove this when we have set a saner time zone server-side.
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SET time_zone = '+00:00';";
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }

            return connection;
        }

        private static string getConnectionString()
        {
            string connectionString = Environment.GetEnvironmentVariable("DB_CONNECTION_STRING") ?? String.Empty;

            if (string.IsNullOrEmpty(connectionString))
            {
                string host = (Environment.GetEnvironmentVariable("DB_HOST") ?? "localhost");
                string port = (Environment.GetEnvironmentVariable("DB_PORT") ?? "3306");
                string user = (Environment.GetEnvironmentVariable("DB_USER") ?? "root");
                string password = (Environment.GetEnvironmentVariable("DB_PASS") ?? string.Empty);
                string name = (Environment.GetEnvironmentVariable("DB_NAME") ?? "osu");
                bool pooling = Environment.GetEnvironmentVariable("DB_POOLING") != "0";
                int maxPoolSize = int.Parse(Environment.GetEnvironmentVariable("DB_MAX_POOL_SIZE") ?? "100");

                string passwordString = string.IsNullOrEmpty(password) ? string.Empty : $"Password={password};";

                // Pipelining disabled because ProxySQL no like.
                connectionString =
                    $"Server={host};Port={port};Database={name};User ID={user};{passwordString}ConnectionTimeout=5;ConnectionReset=false;Pooling={pooling};Max Pool Size={maxPoolSize}; Pipelining=false";
            }

            return connectionString;
        }
    }
}
