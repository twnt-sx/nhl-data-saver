import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    concat,
    date_trunc,
    explode,
    input_file_name,
    regexp_extract,
    to_utc_timestamp,
    transform,
)

from config import TEAM_ABBRS, TEAM_ROSTERS_PREFIX
from services.clickhouse_manager import ClickhouseManager
from services.s3_manager import S3Manager
from spark_utils import normalize_roster_players_struct, unidecode_udf, write_to_clickhouse
from sql import (
    TEAM_META_DATA_QUERY,
    TEAM_ROSTERS_LOCAL_DDL,
    TEAM_ROSTERS_DISTRIBUTED_DDL,
    TEAM_ROSTERS_TEMP_DDL,
    INSERT_INTO_TEAM_ROSTERS
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
team_meta_table = 'team_meta'
team_rosters_table = 'team_rosters'


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

        logger.info('📁 Поиск последней сохраненной папки с составами команд')
        last_created_prefix = max(s3_manager.get_subprefix_last_segments(TEAM_ROSTERS_PREFIX))
        logger.info(f'✔️ Была найдена папка {last_created_prefix}')

        team_count = ch_manager.count_column(team_meta_table, 'team_id', distinct=True)
        logger.info(f'#️⃣ Количество команд в таблице {team_meta_table}: {team_count}, в конфигах: {len(TEAM_ABBRS)}')
        if team_count != len(TEAM_ABBRS):
            raise RuntimeError('❌ Несоответствие количества команд в базе данных и в конфигах')

        logger.info('✴️ Создание SparkSession')
        spark = (
            SparkSession
            .builder
            .appName('TransformTeamRosters')
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel('WARN')
        logger.info(f'✔️ SparkSession "{spark.sparkContext.appName}" создана')

        team_meta_pandas_df = ch_manager.query_pandas_df(TEAM_META_DATA_QUERY.format(db=clickhouse_db))
        team_meta_spark_df = spark.createDataFrame(team_meta_pandas_df)

        logger.info('📁 Чтение данных из S3')
        all_rosters_df = (
            spark
            .read
            .parquet(f's3a://{minio_bucket_name}/{TEAM_ROSTERS_PREFIX}/{last_created_prefix}/*.parquet')
        )
        logger.info('✔️ Данные из S3 прочитаны')

        all_rosters_df = (
            all_rosters_df
            .withColumn('source_file', input_file_name())
            .withColumns({
                'team_abbrev': regexp_extract(col('source_file'), r'([A-Z]{3}).*\.parquet', 1),
                'players': concat(
                    transform('forwards', lambda x: normalize_roster_players_struct(x)),
                    transform('defensemen', lambda x: normalize_roster_players_struct(x)),
                    transform('goalies', lambda x: normalize_roster_players_struct(x))
                )
            })
            .join(broadcast(team_meta_spark_df), on='team_abbrev', how='inner')
            .withColumn(
                'player', explode('players')
            )
            .select(
                'team_id',
                'team_abbrev',
                'team_name',
                'player.player_id',
                unidecode_udf(col('player.player_full_name')).alias('player_full_name'),
                'player.position',
                'player.sweater_number',
                date_trunc('second', to_utc_timestamp(all_rosters_df.updated_at, 'UTC')).alias('updated_at')
            )
        )

        logger.info('🧾 Подготовка таблиц в базе данных')
        ch_manager.execute(TEAM_ROSTERS_LOCAL_DDL.format(db=clickhouse_db))
        ch_manager.execute(TEAM_ROSTERS_DISTRIBUTED_DDL.format(db=clickhouse_db))
        ch_manager.execute(TEAM_ROSTERS_TEMP_DDL.format(db=clickhouse_db))

        logger.info('📁 Запись трансформированных данных в базу')
        write_to_clickhouse(
            df=all_rosters_df,
            url=clickhouse_jdbc_url,
            user=clickhouse_user,
            password=clickhouse_password,
            dbtable=team_rosters_table
        )

        temp_count = ch_manager.count_column(f'{team_rosters_table}_temp')
        logger.info(f'#️⃣ Строк сохранено в темповой таблице: {temp_count}')
        if not temp_count:
            logger.warning('⚠️ В темповой таблице нет данных для основных таблиц')
            return

        ch_manager.truncate_table(f'{team_rosters_table}_local')
        ch_manager.execute(INSERT_INTO_TEAM_ROSTERS.format(db=clickhouse_db))
        ch_manager.drop_table(f'{team_rosters_table}_temp', on_cluster=False)
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
