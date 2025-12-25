import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    date_trunc,
    to_date,
    to_utc_timestamp
)

from config import PLAYERS_PREFIX
from services.clickhouse_manager import ClickhouseManager
from spark_schemas import PLAYERS_SCHEMA
from spark_utils import unidecode_udf, write_to_clickhouse
from sql import PLAYERS_LOCAL_DDL, PLAYERS_DISTRIBUTED_DDL

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

minio_bucket_name = os.getenv('MINIO_BUCKET_NAME')
clickhouse_jdbc_url = os.getenv("CLICKHOUSE_JDBC_URL")
clickhouse_host = os.getenv("CLICKHOUSE_HOST")
clickhouse_user = os.getenv("CLICKHOUSE_USER")
clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD")
clickhouse_db = os.getenv('CLICKHOUSE_DB')
players_table = 'players'


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
            .appName('TransformPlayers')
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel('WARN')
        logger.info(f'✔️ SparkSession "{spark.sparkContext.appName}" создана')

        logger.info('📁 Чтение данных из S3')
        players_df = (
            spark
            .read
            .schema(PLAYERS_SCHEMA)
            .parquet(f's3a://{minio_bucket_name}/{PLAYERS_PREFIX}/*.parquet')
        )
        logger.info('✔️ Данные из S3 прочитаны')

        players_df = (
            players_df
            .withColumnsRenamed({
                'birthCity.default': 'birth_city',
                'fullTeamName.default': 'current_team_name',
                'firstName.default': 'first_name',
                'lastName.default': 'last_name'
            })
            .select(
                col('playerId').alias('player_id'),
                col('isActive').alias('is_active'),
                unidecode_udf(
                    concat_ws(
                        ' ',
                        'first_name',
                        'last_name'
                    )
                ).alias('player_full_name'),
                to_date(col('birthDate')).alias('birth_date'),
                unidecode_udf(col('birth_city')).alias('birth_city'),
                col('birthCountry').alias('birth_country'),
                col('heightInCentimeters').alias('height_cm'),
                col('weightInKilograms').alias('weight_kg'),
                col('headshot').alias('headshot_img'),
                col('heroImage').alias('hero_img'),
                'position',
                col('sweaterNumber').alias('sweater_number'),
                col('shootsCatches').alias('shoots_catches'),
                col('currentTeamId').alias('current_team_id'),
                col('currentTeamAbbrev').alias('current_team_abbrev'),
                unidecode_udf(col('current_team_name')).alias('current_team_name'),
                date_trunc('second', to_utc_timestamp('updated_at', 'UTC')).alias('updated_at')
            )
        )

        ch_manager.execute(PLAYERS_LOCAL_DDL.format(db=clickhouse_db))
        ch_manager.execute(PLAYERS_DISTRIBUTED_DDL.format(db=clickhouse_db))

        logger.info('📁 Запись трансформированных данных в базу')
        write_to_clickhouse(
            df=players_df,
            url=clickhouse_jdbc_url,
            user=clickhouse_user,
            password=clickhouse_password,
            dbtable=players_table,
            coalesce=4
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
