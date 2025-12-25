import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, explode, to_utc_timestamp

from config import CONFERENCES,DIVISIONS, TEAM_META_PREFIX
from services.clickhouse_manager import ClickhouseManager
from services.s3_manager import S3Manager
from spark_schemas import CONFERENCES_SCHEMA, DIVISIONS_SCHEMA
from spark_utils import unidecode_udf, write_to_clickhouse
from sql import TEAM_META_LOCAL_DDL, TEAM_META_DISTRIBUTED_DDL
from utils import get_current_utc_ts

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

minio_root_user = os.getenv('MINIO_ROOT_USER')
minio_root_password = os.getenv('MINIO_ROOT_PASSWORD')
minio_bucket_name = os.getenv('MINIO_BUCKET_NAME')
clickhouse_jdbc_url = os.getenv('CLICKHOUSE_JDBC_URL')
clickhouse_host = os.getenv('CLICKHOUSE_HOST')
clickhouse_user = os.getenv('CLICKHOUSE_USER')
clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD')
clickhouse_db = os.getenv('CLICKHOUSE_DB')
team_meta_table = 'team_meta'


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

        logger.info('📑 Поиск последнего сохраненного файла по Team Meta')
        filepath, modified_at = s3_manager.get_last_modified_file(TEAM_META_PREFIX)
        logger.info(f'✔️ Файл был найден по пути {filepath}. Время изменения файла {modified_at}')

        ch_manager.execute(TEAM_META_LOCAL_DDL.format(db=clickhouse_db))
        ch_manager.execute(TEAM_META_DISTRIBUTED_DDL.format(db=clickhouse_db))

        team_meta_total_records = ch_manager.count_column(team_meta_table)
        if team_meta_total_records and (get_current_utc_ts() - modified_at).days >= 1:
            logger.info('⚠️ Данные по Team Meta уже хранятся в базе. Файл в S3 не менялся')
            return

        logger.info('✴️ Создание SparkSession')
        spark = (
            SparkSession
            .builder
            .appName('TransformTeamMeta')
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel('WARN')
        logger.info(f'✔️ SparkSession "{spark.sparkContext.appName}" создана')

        logger.info('📁 Чтение данных из S3')
        team_meta_df = (
            spark
            .read
            .parquet(f's3a://{minio_bucket_name}/{filepath}')
        )
        logger.info('✔️ Данные из S3 прочитаны')

        team_meta_df = (
            team_meta_df
            .withColumn(
                'team',
                explode(col('teams'))
            )
            .select(
                col('team.teamId').alias('team_id'),
                col('team.tricode').alias('team_abbrev'),
                col('team.name.default').alias('team_name'),
                date_trunc('second', to_utc_timestamp('updated_at', 'UTC')).alias('updated_at')
            )
        )

        conferences_df = spark.createDataFrame(
            [(conference, team) for conference, teams in CONFERENCES.items() for team in teams],
            schema=CONFERENCES_SCHEMA
        )

        divisions_df = spark.createDataFrame(
            [(division, team) for division, teams in DIVISIONS.items() for team in teams],
            schema=DIVISIONS_SCHEMA
        )

        team_meta_df = (
            team_meta_df
            .join(conferences_df, on='team_abbrev', how='inner')
            .join(divisions_df, on='team_abbrev', how='inner')
            .select(
                'team_id',
                'team_abbrev',
                unidecode_udf(col('team_name')).alias('team_name'),
                'conference',
                'division',
                'updated_at'
            )
        )

        logger.info('📁 Запись трансформированных данных в базу')
        write_to_clickhouse(
            df=team_meta_df,
            url=clickhouse_jdbc_url,
            user=clickhouse_user,
            password=clickhouse_password,
            dbtable=team_meta_table
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
