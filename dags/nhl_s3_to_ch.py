from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from config import (
    CLICKHOUSE_DB,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_USER,
    MINIO_BUCKET_NAME,
    MINIO_ROOT_PASSWORD,
    MINIO_ROOT_USER,
    TELEGRAM_CONN_ID
)
from utils import telegram_conn_exists

default_args = {
    'owner': 'Yegor U',
    'start_date': datetime(2025, 12, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=10)
}

env_vars = {
    'MINIO_ROOT_USER': MINIO_ROOT_USER,
    'MINIO_ROOT_PASSWORD': MINIO_ROOT_PASSWORD,
    'MINIO_BUCKET_NAME': MINIO_BUCKET_NAME,
    'CLICKHOUSE_JDBC_URL': f'jdbc:clickhouse://clickhouse-01:8123/{CLICKHOUSE_DB}',
    'CLICKHOUSE_HOST': 'clickhouse-01',
    'CLICKHOUSE_USER': CLICKHOUSE_USER,
    'CLICKHOUSE_PASSWORD': CLICKHOUSE_PASSWORD,
    'CLICKHOUSE_DB': CLICKHOUSE_DB,
    'PYTHONPATH': '/opt/airflow/src'
}

conf = {
    'spark.executor.instances': '1',
    'spark.executor.memory': '2g',
    'spark.executor.cores': '1',
    'spark.driver.memory': '1g',
    'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
    'spark.hadoop.fs.s3a.access.key': MINIO_ROOT_USER,
    'spark.hadoop.fs.s3a.secret.key': MINIO_ROOT_PASSWORD,
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem'
}

packages = (
    'org.apache.hadoop:hadoop-aws:3.3.4,'
    'com.amazonaws:aws-java-sdk-bundle:1.12.262,'
    'com.clickhouse:clickhouse-jdbc:0.8.2'
)

with DAG(
    dag_id='nhl_s3_to_ch',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Трансформация сырых данных НХЛ и сохранение в Clickhouse',
    tags=['nhl']
) as dag:

    transform_team_meta = SparkSubmitOperator(
        task_id='transform_team_meta',
        application="/opt/airflow/src/scripts/transform_team_meta.py",
        conn_id='spark_default',
        env_vars=env_vars,
        conf=conf,
        packages=packages
    )

    transform_team_rosters = SparkSubmitOperator(
        task_id='transform_team_rosters',
        application="/opt/airflow/src/scripts/transform_team_rosters.py",
        conn_id='spark_default',
        env_vars=env_vars,
        conf=conf,
        packages=packages
    )

    transform_schedule = SparkSubmitOperator(
        task_id='transform_schedule',
        application="/opt/airflow/src/scripts/transform_schedule.py",
        conn_id='spark_default',
        env_vars=env_vars,
        conf=conf,
        packages=packages
    )

    transform_players = SparkSubmitOperator(
        task_id='transform_players',
        application="/opt/airflow/src/scripts/transform_players.py",
        conn_id='spark_default',
        env_vars=env_vars,
        conf=conf,
        packages=packages
    )

    transform_game_stories = SparkSubmitOperator(
        task_id='transform_game_stories',
        application="/opt/airflow/src/scripts/transform_game_stories.py",
        conn_id='spark_default',
        env_vars=env_vars,
        conf=conf,
        packages=packages
    )

    transform_boxscores = SparkSubmitOperator(
        task_id='transform_boxscores',
        application="/opt/airflow/src/scripts/transform_boxscores.py",
        conn_id='spark_default',
        env_vars=env_vars,
        conf=conf,
        packages=packages
    )

    check_telegram_connection = ShortCircuitOperator(
        task_id="check_telegram_connection",
        python_callable=lambda: telegram_conn_exists(TELEGRAM_CONN_ID)
    )

    send_telegram_message = TelegramOperator(
        task_id='send_telegram_message',
        telegram_conn_id=TELEGRAM_CONN_ID,
        text=(
            "🍻 DAG nhl_s3_to_ch отработал успешно\n"
            "Дата запуска: {{ macros.datetime.now().strftime('%d.%m.%Y') }}\n"
        )
    )

    transform_team_meta >> transform_team_rosters >> \
    [transform_schedule, transform_players] >> transform_game_stories >> transform_boxscores >> \
    check_telegram_connection >> send_telegram_message