import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    date_trunc,
    explode,
    to_utc_timestamp
)

from config import REGULAR_SEASON_SCHEDULE_PREFIX, VALID_GAME_TYPES
from services.clickhouse_manager import ClickhouseManager
from spark_utils import unidecode_udf, write_to_clickhouse
from sql import SCHEDULE_LOCAL_DDL, SCHEDULE_DISTRIBUTED_DDL

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

minio_bucket_name = os.getenv('MINIO_BUCKET_NAME')
clickhouse_jdbc_url = os.getenv("CLICKHOUSE_JDBC_URL")
clickhouse_host = os.getenv("CLICKHOUSE_HOST")
clickhouse_user = os.getenv("CLICKHOUSE_USER")
clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD")
clickhouse_db = os.getenv('CLICKHOUSE_DB')
schedule_table = 'schedule'


def main():
    ch_manager = None
    spark = None

    try:
        ch_manager = ClickhouseManager(
            host=clickhouse_host,
            port=8123,
            username=clickhouse_user,
            password=clickhouse_password,
            database=clickhouse_db
        )

        logger.info('✴️ Создание SparkSession')
        spark = (
            SparkSession
            .builder
            .appName('TransformSchedule')
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel('WARN')
        logger.info(f'✔️ SparkSession "{spark.sparkContext.appName}" создана')

        ch_manager.execute(SCHEDULE_LOCAL_DDL.format(db=clickhouse_db))
        ch_manager.execute(SCHEDULE_DISTRIBUTED_DDL.format(db=clickhouse_db))

        games_to_exclude = ch_manager.query_game_ids(schedule_table, where='WHERE game_state = \'OFF\'')

        logger.info('📁 Чтение данных из S3')
        schedule_df = (
            spark
            .read
            .parquet(f's3a://{minio_bucket_name}/{REGULAR_SEASON_SCHEDULE_PREFIX}/*.parquet')
            .withColumn('game_day', explode(col('gameWeek')))
            .select('game_day', 'updated_at')
        )
        logger.info('✔️ Данные из S3 прочитаны')

        all_games_df = (
            schedule_df
            .withColumn('game', explode(col('game_day.games')))
            .select('game', 'updated_at')
            .where(
                col('game.gameType').isin(VALID_GAME_TYPES)
                & ~col('game.id').isin([row[0] for row in games_to_exclude.result_rows])
            )
            .cache()
        )

        if all_games_df.isEmpty():
            logger.info('⚠️ Отсутствуют незавершенные игры для обновления информации')
            return

        games_to_insert_df = (
            all_games_df
            .select(
                col('game.id').alias('game_id'),
                col('game.season').alias('season'),
                date_trunc('second', to_utc_timestamp('game.startTimeUTC', 'UTC')).alias('start_time'),
                col('game.gameType').alias('game_type'),
                col('game.venue.default').alias('venue'),
                col('game.homeTeam.id').alias('home_team_id'),
                col('game.homeTeam.abbrev').alias('home_team_abbrev'),
                unidecode_udf(
                    concat_ws(
                        ' ',
                        col('game.homeTeam.placeName.default'),
                        col('game.homeTeam.commonName.default')
                    )
                ).alias('home_team_name'),
                col('game.awayTeam.id').alias('away_team_id'),
                col('game.awayTeam.abbrev').alias('away_team_abbrev'),
                unidecode_udf(
                    concat_ws(
                        ' ',
                        col('game.awayTeam.placeName.default'),
                        col('game.awayTeam.commonName.default')
                    )
                ).alias('away_team_name'),
                col('game.gameState').alias('game_state'),
                col('game.gameScheduleState').alias('game_schedule_state'),
                date_trunc('second', to_utc_timestamp('updated_at', 'UTC')).alias('updated_at')
            )
        )

        logger.info('📁 Запись трансформированных данных в базу')
        write_to_clickhouse(
            df=games_to_insert_df,
            url=clickhouse_jdbc_url,
            user=clickhouse_user,
            password=clickhouse_password,
            dbtable=schedule_table
        )

        logger.info('✔️ Данные записаны в основные таблицы')
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