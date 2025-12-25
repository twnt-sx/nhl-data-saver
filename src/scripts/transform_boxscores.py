import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    concat_ws,
    date_trunc,
    explode,
    lit,
    to_utc_timestamp,
    transform,
)

from config import PLAYED_GAMES_PREFIX
from services.clickhouse_manager import ClickhouseManager
from services.s3_manager import S3Manager
from spark_utils import (
    calculate_seconds_on_ice,
    normalize_goalie_game_stats_struct,
    normalize_skater_game_stats_struct,
    unidecode_udf,
    write_to_clickhouse
)
from sql import (
    GOALIE_GAME_STATS_LOCAL_DDL,
    GOALIE_GAME_STATS_DISTRIBUTED_DDL,
    SKATER_GAME_STATS_LOCAL_DDL,
    SKATER_GAME_STATS_DISTRIBUTED_DDL
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
skater_game_stats_table = 'skater_game_stats'
goalie_game_stats_table = 'goalie_game_stats'


def create_boxscores_df(spark_session: SparkSession, game_ids: set) -> DataFrame:
    """
    Возвращает датафрейм с необходимыми для дальнейшей логики полями по играм, которые еще не сохранены в БД.
    Датафрейм будет использоваться для создания матчевой личной статистики игроков (отдельно для полевых
    и голкиперов)
    """

    paths = (
        f's3a://{minio_bucket_name}/{PLAYED_GAMES_PREFIX}/{game_id}/boxscore.parquet'
        for game_id
        in game_ids
    )

    logger.info('📁 Чтение данных из S3')
    raw_boxscores_df = (
        spark_session
        .read
        .parquet(*paths)
    )
    logger.info('✔️ Данные из S3 прочитаны')

    raw_boxscores_df = (
        raw_boxscores_df
        .withColumnsRenamed({
            'homeTeam.id': 'home_team_id',
            'homeTeam.placeName.default': 'home_team_placename',
            'homeTeam.commonName.default': 'home_team_name',
            'homeTeam.abbrev': 'home_team_abbrev',
            'awayTeam.id': 'away_team_id',
            'awayTeam.placeName.default': 'away_team_placename',
            'awayTeam.commonName.default': 'away_team_name',
            'awayTeam.abbrev': 'away_team_abbrev',
            'playerByGameStats.homeTeam.forwards': 'home_team_forwards_stats',
            'playerByGameStats.homeTeam.defense': 'home_team_defense_stats',
            'playerByGameStats.homeTeam.goalies': 'home_team_goalies_stats',
            'playerByGameStats.awayTeam.forwards': 'away_team_forwards_stats',
            'playerByGameStats.awayTeam.defense': 'away_team_defense_stats',
            'playerByGameStats.awayTeam.goalies': 'away_team_goalies_stats'
        })
        .select(
            col('id').alias('game_id'),
            'season',
            date_trunc('second', to_utc_timestamp('startTimeUTC', 'UTC')).alias('start_time'),
            'home_team_id',
            'home_team_abbrev',
            concat_ws(' ', 'home_team_placename', 'home_team_name').alias('home_team_name'),
            'away_team_id',
            'away_team_abbrev',
            concat_ws(' ', 'away_team_placename', 'away_team_name').alias('away_team_name'),
            'home_team_forwards_stats',
            'home_team_defense_stats',
            'home_team_goalies_stats',
            'away_team_forwards_stats',
            'away_team_defense_stats',
            'away_team_goalies_stats',
            date_trunc('second', to_utc_timestamp('updated_at', 'UTC')).alias('updated_at')
        )
    )

    home_team_df = (
        raw_boxscores_df
        .select(
            'game_id',
            'season',
            'start_time',
            lit('home').alias('team_type'),
            col('home_team_id').alias('team_id'),
            col('home_team_abbrev').alias('team_abbrev'),
            col('home_team_name').alias('team_name'),
            concat(
                transform('home_team_forwards_stats', lambda x: normalize_skater_game_stats_struct(x)),
                transform('home_team_defense_stats', lambda x: normalize_skater_game_stats_struct(x)),
            ).alias('skaters_stats'),
            transform(
                'home_team_goalies_stats', lambda x: normalize_goalie_game_stats_struct(x)
            ).alias('goalies_stats'),
            'updated_at'
        )
    )

    away_team_df = (
        raw_boxscores_df
        .select(
            'game_id',
            'season',
            'start_time',
            lit('away').alias('team_type'),
            col('away_team_id').alias('team_id'),
            col('away_team_abbrev').alias('team_abbrev'),
            col('away_team_name').alias('team_name'),
            concat(
                transform('away_team_forwards_stats', lambda x: normalize_skater_game_stats_struct(x)),
                transform('away_team_defense_stats', lambda x: normalize_skater_game_stats_struct(x)),
            ).alias('skaters_stats'),
            transform(
                'away_team_goalies_stats', lambda x: normalize_goalie_game_stats_struct(x)
            ).alias('goalies_stats'),
            'updated_at'
        )
    )

    return home_team_df.unionByName(away_team_df)


def create_skater_game_stats_df(separated_by_teams_df: DataFrame, game_ids: set) -> DataFrame:
    skater_game_stats_df = (
        separated_by_teams_df
        .where(col('game_id').isin(sorted(game_ids)))
        .withColumn(
            'skater_stats',
            explode('skaters_stats')
        )
        .select(
            'game_id',
            'season',
            'start_time',
            'team_type',
            'team_id',
            'team_abbrev',
            unidecode_udf(col('team_name')).alias('team_name'),
            'skater_stats.player_id',
            unidecode_udf(col('skater_stats.player_name')).alias('player_name'),
            'skater_stats.position',
            'skater_stats.sweater_number',
            'skater_stats.goals',
            'skater_stats.assists',
            'skater_stats.points',
            'skater_stats.plus_minus',
            'skater_stats.penalty_minutes',
            'skater_stats.power_play_goals',
            'skater_stats.shots',
            'skater_stats.hits',
            'skater_stats.blocked_shots',
            'skater_stats.faceoff_winning_pctg',
            'skater_stats.giveaways',
            'skater_stats.takeaways',
            'skater_stats.shifts',
            'skater_stats.time_on_ice',
            calculate_seconds_on_ice(col('skater_stats.time_on_ice')).alias('time_on_ice_seconds'),
            'updated_at'
        )
    )

    return skater_game_stats_df


def create_goalie_game_stats_df(separated_by_teams_df: DataFrame, game_ids: set) -> DataFrame:
    goalie_game_stats_df = (
        separated_by_teams_df
        .where(col('game_id').isin(sorted(game_ids)))
        .withColumn(
            'goalie_stats',
            explode('goalies_stats')
        )
        .select(
            'game_id',
            'season',
            'start_time',
            'team_type',
            'team_id',
            'team_abbrev',
            unidecode_udf(col('team_name')).alias('team_name'),
            'goalie_stats.player_id',
            unidecode_udf(col('goalie_stats.player_name')).alias('player_name'),
            'goalie_stats.position',
            'goalie_stats.sweater_number',
            'goalie_stats.starter',
            'goalie_stats.decision',
            'goalie_stats.saves',
            'goalie_stats.shots_against',
            'goalie_stats.save_pctg',
            'goalie_stats.goals_against',
            'goalie_stats.penalty_minutes',
            'goalie_stats.time_on_ice',
            calculate_seconds_on_ice(col('goalie_stats.time_on_ice')).alias('time_on_ice_seconds'),
            'updated_at'
        )
    )

    return goalie_game_stats_df


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

        logger.info('🧾 Подготовка таблиц со статистикой полевых игроков')
        ch_manager.execute(SKATER_GAME_STATS_LOCAL_DDL.format(db=clickhouse_db))
        ch_manager.execute(SKATER_GAME_STATS_DISTRIBUTED_DDL.format(db=clickhouse_db))
        logger.info('🧾 Подготовка таблиц со статистикой голкиперов')
        ch_manager.execute(GOALIE_GAME_STATS_LOCAL_DDL.format(db=clickhouse_db))
        ch_manager.execute(GOALIE_GAME_STATS_DISTRIBUTED_DDL.format(db=clickhouse_db))

        s3_played_game_ids = set(map(int, s3_manager.get_subprefix_last_segments(PLAYED_GAMES_PREFIX)))
        game_ids_to_exclude_sk = {
            row[0]
            for row
            in ch_manager.query_game_ids(skater_game_stats_table).result_rows
        }
        game_ids_to_exclude_gl = {
            row[0]
            for row
            in ch_manager.query_game_ids(goalie_game_stats_table).result_rows
        }

        all_game_ids_to_include = s3_played_game_ids - (game_ids_to_exclude_sk | game_ids_to_exclude_gl)
        if not all_game_ids_to_include:
            logger.info('⚠️ Отсутствуют завершенные игры для добавления в таблицы')
            return

        logger.info('✴️ Создание SparkSession')
        spark = (
            SparkSession
            .builder
            .appName('TransformBoxscores')
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel('WARN')
        logger.info(f'✔️ SparkSession "{spark.sparkContext.appName}" создана')

        logger.info('⏏️ Формирование boxscores_df из сырых данных')
        boxscores_df = create_boxscores_df(spark, all_game_ids_to_include).cache()

        game_ids_to_include_sk = all_game_ids_to_include - game_ids_to_exclude_sk
        if game_ids_to_include_sk:
            logger.info('⏏️ Формирование skater_game_stats_df')
            skater_game_stats_df = create_skater_game_stats_df(boxscores_df, game_ids_to_include_sk)

            logger.info(f'📁 Запись трансформированных данных в таблицу {skater_game_stats_table}')
            write_to_clickhouse(
                df=skater_game_stats_df,
                url=clickhouse_jdbc_url,
                user=clickhouse_user,
                password=clickhouse_password,
                dbtable=skater_game_stats_table,
                batchsize=5000,
                coalesce=4
            )
            logger.info(f'✔️ Данные записаны в таблицу {skater_game_stats_table}')
        else:
            logger.info(f'⚠️ Отсутствуют завершенные игры для добавления в таблицу {skater_game_stats_table}')

        game_ids_to_include_gl = all_game_ids_to_include - game_ids_to_exclude_gl
        if game_ids_to_include_gl:
            logger.info('⏏️ Формирование goalie_game_stats_df')
            goalie_game_stats_df = create_goalie_game_stats_df(boxscores_df, game_ids_to_include_gl)

            logger.info(f'📁 Запись трансформированных данных в таблицу {goalie_game_stats_table}')
            write_to_clickhouse(
                df=goalie_game_stats_df,
                url=clickhouse_jdbc_url,
                user=clickhouse_user,
                password=clickhouse_password,
                dbtable=skater_game_stats_table,
                coalesce=4
            )
            logger.info(f'✔️ Данные записаны в таблицу {goalie_game_stats_table}')
        else:
            logger.info(f'⚠️ Отсутствуют завершенные игры для добавления в таблицу {goalie_game_stats_table}')

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