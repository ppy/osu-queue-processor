using System;
using System.Collections.Generic;
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

    internal class ManiaKeyRankingProcessor : QueueProcessor<long>
    {
        public ManiaKeyRankingProcessor()
            : base(new QueueConfiguration { InputQueueName = "mania4k7k" })
        {
        }

        protected override void ProcessResult(long item)
        {
            using (var db = GetDatabaseConnection())
            {
                var row = db.QueryFirstOrDefault("SELECT * FROM osu_scores_mania_high WHERE score_id = @item", new { item });

                if (row == null)
                    // score has been since replaced.
                    return;

                uint user = row.user_id;
                int size = db.QueryFirst<int>($"SELECT diff_size FROM osu_beatmaps WHERE beatmap_id = {row.beatmap_id}");

                if (size != 4 && size != 7)
                    return;

                string newTableName = $"osu_user_stats_mania_{size}k";

                //get all mania scores for current key mode
                var scores = db.Query($"SELECT * FROM osu_scores_mania_high WHERE user_id = {user} AND pp IS NOT NULL AND beatmap_id IN (SELECT beatmap_id FROM osu_beatmaps WHERE playmode = 3 AND diff_size = {size})").ToList();

                (double pp, double accuracy) = getAggregatePerformanceAccuracy(scores);

                int pos = 1 + db.QuerySingle<int>($"SELECT COUNT(*) FROM {newTableName} WHERE rank_score > @pp", new { pp });
                var existingRow = db.QueryFirstOrDefault($"SELECT * FROM {newTableName} WHERE user_id = @user", new { user });

                int xRankCount = scores.Count(s => s.rank == "X");
                int xhRankCount = scores.Count(s => s.rank == "XH");
                int sRankCount = scores.Count(s => s.rank == "S");
                int shRankCount = scores.Count(s => s.rank == "SH");
                int aRankCount = scores.Count(s => s.rank == "A");

                // remove self from total if required.
                if (existingRow?.rank_score > pp)
                    // note that as we are processing jobs threaded, this may be off-by-one due to the above being fetched in two queries.
                    pos = Math.Max(1, pos - 1);

                db.Execute($"INSERT {newTableName}" +
                           $"(user_id, country_acronym, playcount, x_rank_count, xh_rank_count, s_rank_count, sh_rank_count, a_rank_count, rank_score, rank_score_index, accuracy_new) " +
                           $"VALUES (@user_id, @country_acronym, 0, @x_rank_count, @xh_rank_count, @s_rank_count, @sh_rank_count, @a_rank_count, @pp, @pos, @accuracy)" +
                           $"ON DUPLICATE KEY UPDATE rank_score = @pp, rank_score_index = @pos, x_rank_count = @x_rank_count, xh_rank_count = @xh_rank_count, s_rank_count = @s_rank_count, sh_rank_count = @sh_rank_count, a_rank_count = @a_rank_count, accuracy_new = @accuracy",
                    new
                    {
                        row.user_id,
                        row.country_acronym,
                        pos,
                        pp,
                        accuracy,
                        x_rank_count = xRankCount,
                        xh_rank_count = xhRankCount,
                        s_rank_count = sRankCount,
                        sh_rank_count = shRankCount,
                        a_rank_count = aRankCount,
                    });
            }
        }

        private (double pp, double accuracy) getAggregatePerformanceAccuracy(IEnumerable<dynamic> scores)
        {
            if (!scores.Any())
                return (0, 100);

            scores = scores.OrderByDescending(s => s.pp);
            scores = scores.GroupBy(s => s.beatmap_id).Select(g => g.First());

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
