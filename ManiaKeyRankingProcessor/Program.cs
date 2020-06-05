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
                           $"(user_id, x_rank_count, xh_rank_count, s_rank_count, sh_rank_count, a_rank_count, ) " +
                           $"VALUES)"
                    
                /*user_id int(11) unsigned not null
                primary key,
                    playcount mediumint(11) not null,
                x_rank_count mediumint not null,
                xh_rank_count mediumint default 0 null,
                s_rank_count mediumint not null,
                sh_rank_count mediumint default 0 null,
                a_rank_count mediumint not null,
                country_acronym char(2) default '' not null,
                rank_score float unsigned not null,
                rank_score_index int unsigned not null,
                accuracy_new float unsigned not null,
                last_update timestamp default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
                    last_played timestamp default CURRENT_TIMESTAMP not null
                    */
                
                Console.WriteLine($"Recalculated user {user} for {size}k ({rows} rows)");
            }
        }
    }
}