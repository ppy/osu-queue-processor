using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Dapper;
using osu.Server.QueueProcessor;

namespace ManiaKeyRankingProcessor
{
    public class Program
    {
        public static void Main(string[] args)
        {
            new ManiaKeyRankingProcessor().Run();
        }
    }

    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class QueueItem
    {
        public int score_id { get; set; }

        public bool high { get; set; }
    }

    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class UserStats
    {
        public long user_id { get; set; }
        public string country_acronym { get; set; }
        public int playcount { get; set; } // only used for initial population
        public int pos { get; set; }
        public double pp { get; set; }
        public double accuracy { get; set; }
        public int x_rank_count { get; set; }
        public int xh_rank_count { get; set; }
        public int s_rank_count { get; set; }
        public int sh_rank_count { get; set; }
        public int a_rank_count { get; set; }
    }

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
                    var scores = db.Query($"SELECT * FROM osu_scores_mania_high WHERE user_id = @user_id AND beatmap_id IN (SELECT beatmap_id FROM osu_beatmaps WHERE playmode = 3 AND diff_size = {keyCount})", stats).ToList();

                    (stats.pp, stats.accuracy) = getAggregatePerformanceAccuracy(scores);

                    stats.pos = 1 + db.QuerySingle<int>($"SELECT COUNT(*) FROM {newTableName} WHERE rank_score > @pp", stats);

                    stats.x_rank_count = scores.Count(s => s.rank == "X");
                    stats.xh_rank_count = scores.Count(s => s.rank == "XH");
                    stats.s_rank_count = scores.Count(s => s.rank == "S");
                    stats.sh_rank_count = scores.Count(s => s.rank == "SH");
                    stats.a_rank_count = scores.Count(s => s.rank == "A");

                    // remove self from total if required.
                    if (existingRow != null)
                    {
                        if (existingRow.rank_score > stats.pp)
                            // note that as we are processing jobs threaded, this may be off-by-one due to the above being fetched in two queries.
                            stats.pos = Math.Max(1, stats.pos - 1);
                    }
                    else
                    {
                        //no existing row, so we are going to insert.
                        stats.country_acronym = scores.FirstOrDefault()?.country_acronym ?? db.QueryFirst<string>("SELECT country_acronym FROM phpbb_users WHERE user_id = @user_id", stats);

                        // make up a rough playcount based on user play distribution.
                        stats.playcount = db.QuerySingle<int?>("SELECT " +
                                                               "(SELECT playcount FROM osu_user_stats_mania WHERE user_id = @user_id) * " +
                                                               $"(SELECT COUNT(*) FROM osu_scores_mania_high WHERE user_id = @user_id AND beatmap_id IN (SELECT beatmap_id FROM osu_beatmaps WHERE diff_size = {keyCount} AND playmode = 3)) / " +
                                                               "(SELECT COUNT(*) FROM osu_scores_mania_high WHERE user_id = @user_id)", stats) ?? 1;
                    }

                    db.Execute($"INSERT INTO {newTableName}" +
                               "(user_id, country_acronym, playcount, x_rank_count, xh_rank_count, s_rank_count, sh_rank_count, a_rank_count, rank_score, rank_score_index, accuracy_new) " +
                               "VALUES (@user_id, @country_acronym, @playcount, @x_rank_count, @xh_rank_count, @s_rank_count, @sh_rank_count, @a_rank_count, @pp, @pos, @accuracy)" +
                               "ON DUPLICATE KEY UPDATE rank_score = @pp, playcount = playcount + 1, rank_score_index = @pos, x_rank_count = @x_rank_count, xh_rank_count = @xh_rank_count, s_rank_count = @s_rank_count, sh_rank_count = @sh_rank_count, a_rank_count = @a_rank_count, accuracy_new = @accuracy", stats);
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

            scores = scores.Where(s => s.pp != null)
                           .OrderByDescending(s => s.pp)
                           .GroupBy(s => s.beatmap_id).Select(g => g.First());
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
