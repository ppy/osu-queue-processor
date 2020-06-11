// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

using System;
using System.Collections.Generic;
using System.Linq;
using Dapper;
using osu.Server.QueueProcessor;

namespace osu.Server.Queues.ManiaKeyRankingProcessor
{
    internal class ManiaKeyRankingProcessor : QueueProcessor<QueueItem>
    {
        public ManiaKeyRankingProcessor()
            : base(new QueueConfiguration { InputQueueName = "mania4k7k" })
        {
        }

        protected override void ProcessResult(QueueItem item)
        {
            using (var db = GetDatabaseConnection())
            {
                var newScore = db.QueryFirstOrDefault($"SELECT * FROM osu_scores_mania{(item.high ? "_high" : "")} WHERE score_id = @score_id", item);

                if (newScore == null)
                    // score has been since replaced.
                    return;

                int keyCount = db.QueryFirst<int>($"SELECT diff_size FROM osu_beatmaps WHERE beatmap_id = {newScore.beatmap_id}");

                if (keyCount != 4 && keyCount != 7)
                    return;

                string newTableName = $"osu_user_stats_mania_{keyCount}k";

                UserStats stats = new UserStats
                {
                    user_id = newScore.user_id,
                };

                var existingRow = db.QueryFirstOrDefault($"SELECT * FROM {newTableName} WHERE user_id = @user_id", stats);

                if (item.high || existingRow == null)
                {
                    //get all mania scores for current key mode
                    var scores = db.Query("SELECT * FROM osu_scores_mania_high "
                                          + "WHERE user_id = @user_id AND pp IS NOT NULL AND beatmap_id IN "
                                          + $"(SELECT beatmap_id FROM osu_beatmaps WHERE playmode = 3 AND diff_size = {keyCount})", stats).ToList();

                    if (scores.Any())
                    {
                        (stats.pp, stats.accuracy) = getAggregatePerformanceAccuracy(scores);

                        stats.pos = 1 + db.QuerySingle<int>($"SELECT COUNT(*) FROM {newTableName} WHERE rank_score > @pp", stats);

                        stats.x_rank_count = scores.Count(s => s.rank == "X");
                        stats.xh_rank_count = scores.Count(s => s.rank == "XH");
                        stats.s_rank_count = scores.Count(s => s.rank == "S");
                        stats.sh_rank_count = scores.Count(s => s.rank == "SH");
                        stats.a_rank_count = scores.Count(s => s.rank == "A");
                    }

                    if (existingRow != null)
                    {
                        // remove self from total if required.
                        // note that as we are processing jobs threaded, this may be off-by-one due to the above being fetched in two queries.
                        if (existingRow.rank_score > stats.pp)
                            stats.pos = Math.Max(1, stats.pos - 1);

                        db.Execute($"UPDATE {newTableName} "
                                   + "SET rank_score = @pp, playcount = playcount + 1, rank_score_index = @pos, accuracy_new = @accuracy, "
                                   + "x_rank_count = @x_rank_count, xh_rank_count = @xh_rank_count, s_rank_count = @s_rank_count, sh_rank_count = @sh_rank_count, a_rank_count = @a_rank_count "
                                   + "WHERE user_id = @user_id", stats);
                    }
                    else
                    {
                        // make up a rough playcount based on user play distribution.
                        stats.playcount = db.QuerySingle<int?>(
                            "SELECT " +
                            "(SELECT playcount FROM osu_user_stats_mania WHERE user_id = @user_id) * " +
                            $"(SELECT COUNT(*) FROM osu_scores_mania_high WHERE user_id = @user_id AND beatmap_id IN (SELECT beatmap_id FROM osu_beatmaps WHERE diff_size = {keyCount} AND playmode = 3)) / " +
                            "(SELECT GREATEST(1, COUNT(*)) FROM osu_scores_mania_high WHERE user_id = @user_id)", stats) ?? 1;

                        db.Execute($"REPLACE INTO {newTableName}"
                                   + "(user_id, country_acronym, playcount, x_rank_count, xh_rank_count, s_rank_count, sh_rank_count, a_rank_count, rank_score, rank_score_index, accuracy_new)"
                                   + "SELECT @user_id, country_acronym, @playcount, @x_rank_count, @xh_rank_count, @s_rank_count, @sh_rank_count, @a_rank_count, @pp, @pos, @accuracy "
                                   + "FROM phpbb_users WHERE user_id = @user_id", stats);
                    }
                }
                else
                {
                    db.Execute($"UPDATE {newTableName} SET playcount = playcount + 1 WHERE user_id = @user_id", stats);
                }
            }
        }

        private (double pp, double accuracy) getAggregatePerformanceAccuracy(IEnumerable<dynamic> scores)
        {
            if (!scores.Any())
                return (0, 100);

            scores = scores.OrderByDescending(s => s.pp)
                           .GroupBy(s => s.beatmap_id).Select(g => g.First()).ToList();

            double factor = 1;
            double pp = 0;

            double accuracy = 0;

            foreach (var s in scores)
            {
                pp += s.pp * factor;

                accuracy +=
                    (double)(s.count50 * 50 + s.count100 * 100 + s.countkatu * 200 + (s.count300 + s.countgeki) * 300) /
                    ((s.count50 + s.count100 + s.count300 + s.countmiss + s.countgeki + s.countkatu) * 300) * factor;

                factor *= 0.95;
            }

            // This weird factor is to keep legacy compatibility with the diminishing bonus of 0.25 by 0.9994 each score
            pp += (417.0 - 1.0 / 3.0) * (1.0 - Math.Pow(0.9994, scores.Count()));

            // We want our accuracy to be normalized. We want the percentage, not a factor in [0, 1], hence we divide 20 by 100
            accuracy *= 100.0 / (20 * (1 - Math.Pow(0.95, scores.Count())));
            return (pp, accuracy);
        }
    }
}
