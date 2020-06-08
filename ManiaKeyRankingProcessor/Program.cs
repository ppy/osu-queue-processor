using System;
using Dapper;
using osu.Framework.Logging;
using osu.Server.QueueProcessor;

namespace ManiaKeyRankingProcessor
{
    class Program
    {
        static void Main(string[] args)
        {
            new ManiaKeyRankingProcessor().Run();
        }
    }

    class ManiaKeyRankingProcessor : QueueProcessor<long>
    {
        public ManiaKeyRankingProcessor() : base(new QueueConfiguration { InputQueueName = "mania4k7k" })
        {
        }

        protected override void ProcessResult(long item)
        {
            Logger.Log($"Processing {item}");
            using (var db = GetDatabaseConnection())
            {
                var row = db.QueryFirst("SELECT * FROM osu_scores_mania_high WHERE score_id = @item", new { item });

                uint user = row.user_id;
                int size = db.QueryFirst<int>($"SELECT diff_size FROM osu_beatmaps WHERE beatmap_id = {row.beatmap_id}");

                if (size != 4 && size != 7)
                    return;

                Console.WriteLine($"Recalculating user {user} for {size}k");

                string newTableName = $"osu_user_stats_mania_{size}k";

                int rows = 0;
                //get all mania scores for current key mode
                foreach (var score in db.Query($"SELECT * FROM osu_scores_mania_high WHERE user_id = {user} AND beatmap_id in (SELECT beatmap_id FROM osu_beatmaps WHERE playmode = 3 AND diff_size = {size})"))
                {
                    rows++;
                }

                db.Execute($"REPLACE INTO {newTableName}" +
                           $"(user_id, country_acronym, playcount, x_rank_count, xh_rank_count, s_rank_count, sh_rank_count, a_rank_count, rank_score, rank_score_index, accuracy_new) " +
                           $"VALUES (@user_id, @country_acronym, @playcount, 0, 0, 0, 0, 0, 0, 0, 1)",
                    new
                    {
                        user_id = user,
                        playcount = rows,
                        country_acronym = row.country_acronym
                    });

                Console.WriteLine($"Recalculated user {user} for {size}k ({rows} rows)");
            }
        }
    }
}