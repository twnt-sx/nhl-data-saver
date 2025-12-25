import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array_sort,
    col,
    collect_list,
    concat_ws,
    date_trunc,
    explode,
    first,
    from_json,
    lit,
    sort_array,
    split,
    struct,
    to_utc_timestamp,
    transform,
    when
)

from config import PLAYED_GAMES_PREFIX
from services.clickhouse_manager import ClickhouseManager
from services.s3_manager import S3Manager
from spark_schemas import GAME_STORY_SCHEMA, SCORING_SCHEMA
from spark_utils import calculate_goal_game_time, unidecode_udf, write_to_clickhouse
from sql import (
    GAME_RESULTS_LOCAL_DDL,
    GAME_RESULTS_DISTRIBUTED_DDL,
    TEAM_GAME_STATS_LOCAL_DDL,
    TEAM_GAME_STATS_DISTRIBUTED_DDL
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

minio_root_user = os.getenv('MINIO_ROOT_USER')
minio_root_password = os.getenv('MINIO_ROOT_PASSWORD')
minio_bucket_name = os.getenv('MINIO_BUCKET_NAME')
clickhouse_jdbc_url = os.getenv("CLICKHOUSE_JDBC_URL")
clickhouse_host = os.getenv("CLICKHOUSE_HOST")
clickhouse_user = os.getenv("CLICKHOUSE_USER")
clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD")
clickhouse_db = os.getenv('CLICKHOUSE_DB')
game_results_table = 'game_results'
team_game_stats_table = 'team_game_stats'


def create_game_stories_df(spark_session: SparkSession, game_ids: set) -> DataFrame:
    """
    Возвращает датафрейм с необходимыми для дальнейшей логики полями по играм, которые еще не сохранены в БД.
    Датафрейм будет использоваться для создания результатов игр и матчевой статистики команд
    """

    paths = (
        f's3a://{minio_bucket_name}/{PLAYED_GAMES_PREFIX}/{game_id}/game_story.parquet'
        for game_id
        in game_ids
    )

    logger.info('📁 Чтение данных из S3')
    game_stories_df = (
        spark_session
        .read
        .schema(GAME_STORY_SCHEMA)
        .parquet(*paths)
    )
    logger.info('✔️ Данные из S3 прочитаны')

    game_stories_df = (
        game_stories_df
        .withColumnsRenamed({
            'awayTeam.id': 'away_team_id',
            'awayTeam.abbrev': 'away_team_abbrev',
            'awayTeam.score': 'away_team_score',
            'homeTeam.id': 'home_team_id',
            'homeTeam.abbrev': 'home_team_abbrev',
            'homeTeam.score': 'home_team_score',
            'awayTeam.placeName.default': 'away_team_placename',
            'awayTeam.name.default': 'away_team_name',
            'homeTeam.placeName.default': 'home_team_placename',
            'homeTeam.name.default': 'home_team_name',
            'summary.scoring': 'scoring_json',
            'summary.threeStars': 'three_stars',
            'summary.teamGameStats': 'team_game_stats',
            'periodDescriptor.periodType': 'period_type'
        })
        .withColumn(
            'scoring',
            from_json(col('scoring_json'), SCORING_SCHEMA)
        )
        .select(
            col('id').alias('game_id'),
            'season',
            col('gameType').alias('game_type'),
            date_trunc('second', to_utc_timestamp('startTimeUTC', 'UTC')).alias('start_time'),
            col('gameState').alias('game_state'),
            col('gameScheduleState').alias('game_schedule_state'),
            'home_team_id',
            'home_team_abbrev',
            concat_ws(' ', 'home_team_placename', 'home_team_name').alias('home_team_name'),
            'home_team_score',
            when(col('home_team_score') > col('away_team_score'), 2)
            .when(col('period_type') != 'REG', 1)
            .otherwise(0)
            .alias('home_team_points'),
            'away_team_id',
            'away_team_abbrev',
            'away_team_score',
            concat_ws(' ', 'away_team_placename', 'away_team_name').alias('away_team_name'),
            when(col('away_team_score') > col('home_team_score'), 2)
            .when(col('period_type') != 'REG', 1)
            .otherwise(0)
            .alias('away_team_points'),
            'scoring',
            'three_stars',
            'team_game_stats',
            'period_type',
            date_trunc('second', to_utc_timestamp('updated_at', 'UTC')).alias('updated_at')
        )
    )

    return game_stories_df


def create_game_results_df(game_stories_df: DataFrame, game_ids: set) -> DataFrame:
    """
    Создает и возвращает датафрейм с результатами игр, включая информацию о голах и трёх звёздах матча
    """

    games_to_include_df = (
        game_stories_df
        .where(col('game_id').isin(sorted(game_ids)))
        .cache()
    )

    scorings_df = (
        games_to_include_df
        .withColumn('scoring_period', explode('scoring'))
        .withColumn('goal', explode('scoring_period.goals'))
        .select(
            'game_id',
            col('goal.playerId').alias('scorer_player_id'),
            col('goal.strength').alias('strength'),
            calculate_goal_game_time(
                col('scoring_period.periodDescriptor.number'),
                col('goal.timeInPeriod')
            ).alias('goal_time'),
            transform('goal.assists', lambda x: x.playerId).alias('assists_player_ids'),
            col('goal.homeScore').alias('home_score'),
            col('goal.awayScore').alias('away_score')
        )
        .groupBy('game_id')
        .agg(
            sort_array(
                collect_list(
                    struct(
                        'goal_time',
                        'home_score',
                        'away_score',
                        'strength',
                        'scorer_player_id',
                        'assists_player_ids'
                    )
                )
            ).alias('goals')
        )
        .select(
            'game_id',
            transform(col('goals'), lambda x: x.goal_time).alias('goals_goal_time'),
            transform(col('goals'), lambda x: x.home_score).alias('goals_home_score'),
            transform(col('goals'), lambda x: x.away_score).alias('goals_away_score'),
            transform(col('goals'), lambda x: x.strength).alias('goals_strength'),
            transform(col('goals'), lambda x: x.scorer_player_id).alias('goals_scorer_player_id'),
            transform(col('goals'), lambda x: x.assists_player_ids).alias('goals_assists_player_ids')
        )
    )

    stars_df = (
        games_to_include_df
        .withColumn(
            'three_stars_sorted',
            array_sort(
                'three_stars',
                lambda l, r:
                when(l.star > r.star, 1)
                .when(l.star < r.star, -1)
                .otherwise(0)
            )
        )
        .withColumn('three_stars_player_ids', transform('three_stars_sorted', lambda x: x.playerId))
        .select('game_id', 'three_stars_player_ids')
    )

    game_results_df = (
        games_to_include_df
        .join(scorings_df, on='game_id', how='inner')
        .join(stars_df, on='game_id', how='inner')
        .select(
            'game_id',
            'season',
            'start_time',
            'period_type',
            'home_team_score',
            'away_team_score',
            'home_team_id',
            'home_team_abbrev',
            unidecode_udf(col('home_team_name')).alias('home_team_name'),
            'away_team_id',
            'away_team_abbrev',
            unidecode_udf(col('away_team_name')).alias('away_team_name'),
            'three_stars_player_ids',
            'goals_goal_time',
            'goals_home_score',
            'goals_away_score',
            'goals_strength',
            'goals_scorer_player_id',
            'goals_assists_player_ids',
            'updated_at'
        )
    )

    games_to_include_df.unpersist()

    return game_results_df


def create_team_game_stats_df(game_stories_df: DataFrame, game_ids: set) -> DataFrame:
    """
    Создает и возвращает датафрейм с матчевой статистикой команд
    """

    games_to_include_df = (
        game_stories_df
        .where(col('game_id').isin(sorted(game_ids)))
        .cache()
    )

    home_stats_df = games_to_include_df.select(
        'game_id',
        'season',
        'start_time',
        col('home_team_id').alias('team_id'),
        col('home_team_abbrev').alias('team_abbrev'),
        col('home_team_name').alias('team_name'),
        lit('home').alias('team_type'),
        col('home_team_score').alias('score'),
        col('home_team_points').alias('points'),
        'period_type',
        'team_game_stats',
        'updated_at'
    )

    away_stats_df = games_to_include_df.select(
        'game_id',
        'season',
        'start_time',
        col('away_team_id').alias('team_id'),
        col('away_team_abbrev').alias('team_abbrev'),
        col('away_team_name').alias('team_name'),
        lit('away').alias('team_type'),
        col('away_team_score').alias('score'),
        col('away_team_points').alias('points'),
        'period_type',
        'team_game_stats',
        'updated_at'
    )

    teams_stats_df = (
        home_stats_df
        .unionByName(away_stats_df)
        .withColumn(
            'team_game_stats',
            transform(
                'team_game_stats',
                lambda x: struct(
                    x.category.alias('category'),
                    when(col('team_type') == 'home', x.homeValue)
                    .otherwise(x.awayValue)
                    .alias('value')
                )
            )
        )
    )

    pivoted_teams_stats_df = (
        teams_stats_df
        .withColumn('stat', explode('team_game_stats'))
        .withColumns({
            'category': col('stat.category'),
            'value': col('stat.value')
        })
        .groupBy(
            'game_id',
            'team_id'
        )
        .pivot('category')
        .agg(first('value'))
    )

    casted_team_game_stats_df = (
        teams_stats_df
        .join(pivoted_teams_stats_df, on=['game_id', 'team_id'], how='inner')
        .withColumns({
            'blocked_shots': col('blockedShots').cast('int'),
            'faceoff_winning_pctg': col('faceoffWinningPctg').cast('float'),
            'giveaways': col('giveaways').cast('int'),
            'hits': col('hits').cast('int'),
            'penalty_minutes': col('pim').cast('int'),
            'power_play_goals': split(col('powerPlay'), '/').getItem(0).cast('int'),
            'times_power_play': split(col('powerPlay'), '/').getItem(1).cast('int'),
            'power_play_pctg': col('powerPlayPctg').cast('float'),
            'shots_on_goal': col('sog').cast('int'),
            'takeaways': col('takeaways').cast('int')
        })
        .cache()
    )

    penalty_kills_df = (
        casted_team_game_stats_df
        .select(
            'game_id',
            when(col('team_type') == 'home', 'away')
            .otherwise('home')
            .alias('team_type'),
            col('times_power_play').alias('times_shorthand'),
            (col('times_power_play') - col('power_play_goals')).alias('penalty_kill'),
            when(col('times_shorthand') == 0, 0)
            .otherwise(col('penalty_kill') / col('times_shorthand'))
            .cast('float')
            .alias('penalty_kill_pctg')
        )
    )

    shots_against_df = (
        casted_team_game_stats_df
        .select(
            'game_id',
            when(col('team_type') == 'home', 'away').otherwise('home').alias('team_type'),
            col('shots_on_goal').alias('shots_against')
        )
    )

    final_team_game_stats_df = (
        casted_team_game_stats_df
        .join(penalty_kills_df, on=['game_id', 'team_type'], how='inner')
        .join(shots_against_df, on=['game_id', 'team_type'], how='inner')
        .select(
            'game_id',
            'season',
            'start_time',
            'team_type',
            'team_id',
            'team_abbrev',
            unidecode_udf(col('team_name')).alias('team_name'),
            'score',
            'points',
            'period_type',
            'shots_on_goal',
            'shots_against',
            'blocked_shots',
            'faceoff_winning_pctg',
            'giveaways',
            'takeaways',
            'hits',
            'penalty_minutes',
            'power_play_goals',
            'times_power_play',
            'power_play_pctg',
            'penalty_kill',
            'times_shorthand',
            'penalty_kill_pctg',
            'updated_at'
        )
    )

    games_to_include_df.unpersist()

    return final_team_game_stats_df


def main():
    ch_manager = None
    spark = None

    try:
        s3_manager = S3Manager(
            endpoint_url='http://minio:9000',
            aws_access_key_id=minio_root_user,
            aws_secret_access_key=minio_root_password,
            bucket=minio_bucket_name
        )

        ch_manager = ClickhouseManager(
            host=clickhouse_host,
            port=8123,
            username=clickhouse_user,
            password=clickhouse_password,
            database=clickhouse_db
        )

        logger.info('🧾 Подготовка таблиц с результатами игр')
        ch_manager.execute(GAME_RESULTS_LOCAL_DDL.format(db=clickhouse_db))
        ch_manager.execute(GAME_RESULTS_DISTRIBUTED_DDL.format(db=clickhouse_db))
        logger.info('🧾 Подготовка таблиц по командной статистике')
        ch_manager.execute(TEAM_GAME_STATS_LOCAL_DDL.format(db=clickhouse_db))
        ch_manager.execute(TEAM_GAME_STATS_DISTRIBUTED_DDL.format(db=clickhouse_db))

        s3_played_game_ids = set(map(int, s3_manager.get_subprefix_last_segments(PLAYED_GAMES_PREFIX)))
        game_ids_to_exclude_gr = {
            row[0]
            for row
            in ch_manager.query_game_ids(game_results_table).result_rows
        }
        game_ids_to_exclude_tgs = {
            row[0]
            for row
            in ch_manager.query_game_ids(team_game_stats_table).result_rows
        }

        all_game_ids_to_include = s3_played_game_ids - (game_ids_to_exclude_gr | game_ids_to_exclude_tgs)
        if not all_game_ids_to_include:
            logger.info('⚠️ Отсутствуют завершенные игры для добавления в таблицы')
            return

        logger.info('✴️ Создание SparkSession')
        spark = (
            SparkSession
            .builder
            .appName('TransformGameStories')
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel('WARN')
        logger.info(f'✔️ SparkSession "{spark.sparkContext.appName}" создана')

        logger.info('⏏️ Формирование game_story_df из сырых данных')
        game_stories_df = create_game_stories_df(spark, all_game_ids_to_include).cache()

        game_ids_to_include_gr = all_game_ids_to_include - game_ids_to_exclude_gr
        if game_ids_to_include_gr:
            logger.info('⏏️ Формирование game_results_df')
            game_results_df = create_game_results_df(game_stories_df, game_ids_to_include_gr)

            logger.info(f'📁 Запись трансформированных данных в таблицу {game_results_table}')
            ch_manager.insert_batches(game_results_df, game_results_table)
            logger.info(f'✔️ Данные записаны в таблицу {game_results_table}')
        else:
            logger.info(f'⚠️ Отсутствуют завершенные игры для добавления в таблицу {game_results_table}')

        game_ids_to_include_tgs = all_game_ids_to_include - game_ids_to_exclude_tgs
        if game_ids_to_include_tgs:
            logger.info('⏏️ Формирование team_game_stats_df')
            team_game_stats_df = create_team_game_stats_df(game_stories_df, game_ids_to_include_tgs)

            logger.info(f'📁 Запись трансформированных данных в таблицу {team_game_stats_table}')
            write_to_clickhouse(
                df=team_game_stats_df,
                url=clickhouse_jdbc_url,
                user=clickhouse_user,
                password=clickhouse_password,
                dbtable=team_game_stats_table,
                coalesce=4
            )
            logger.info(f'✔️ Данные записаны в таблицу {team_game_stats_table}')
        else:
            logger.info(f'⚠️ Отсутствуют завершенные игры для добавления в таблицу {team_game_stats_table}')

    except Exception:
        logger.exception(f'❌ Произошла ошибка')
        raise
    finally:
        if ch_manager is not None:
            ch_manager.close()
        if spark is not None:
            spark.stop()
        logger.info(f'✅ Завершение процесса')


if __name__ == '__main__':
    main()
