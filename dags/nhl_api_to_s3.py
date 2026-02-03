import json
import logging
from datetime import datetime, timedelta
from itertools import chain

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator

from config import (
    CURR_SEASON,
    MINIO_BUCKET_NAME,
    MINIO_ROOT_PASSWORD,
    MINIO_ROOT_USER,
    PLAYED_GAMES_PREFIX,
    PLAYERS_PREFIX,
    REGULAR_SEASON_END_DATE,
    REGULAR_SEASON_SCHEDULE_PREFIX,
    REGULAR_SEASON_START_DATE,
    TEAM_ABBRS,
    TEAM_META_PREFIX,
    TEAM_ROSTERS_PREFIX,
    TELEGRAM_CONN_ID,
    VALID_GAME_TYPES
)
from services.s3_manager import S3Manager
from utils import get_current_utc_ts, send_get_request, telegram_conn_exists


logger = logging.getLogger(__name__)

BASE_URL = 'https://api-web.nhle.com'
TEAM_META_INFORMATION_ENDPOINT = '/v1/meta?teams={team_abbrs}'
TEAM_ROSTER_ENDPOINT = '/v1/roster/{team_abbr}/{season}'
SCHEDULE_BY_DATE_ENDPOINT = '/v1/schedule/{date}'
GAME_PLAY_BY_PLAY_ENDPOINT = '/v1/gamecenter/{game_id}/play-by-play'
GAME_BOXSCORE_ENDPOINT = '/v1/gamecenter/{game_id}/boxscore'
GAME_STORY_ENDPOINT = '/v1/wsc/game-story/{game_id}'
PLAYER_INFO_ENDPOINT = '/v1/player/{player_id}/landing'

s3_manager = S3Manager(
    endpoint_url='http://minio:9000',
    aws_access_key_id=MINIO_ROOT_USER,
    aws_secret_access_key=MINIO_ROOT_PASSWORD,
    bucket=MINIO_BUCKET_NAME
)

def download_team_meta():
    """
    Скачивает метаданные команд. Если метаданные уже сохранены в S3, то не делает вызов API
    """

    last_modified_file = s3_manager.get_last_modified_file(TEAM_META_PREFIX)
    if last_modified_file is not None:
        file, _ = last_modified_file
        logger.info(f'📁 Метаданные команд уже загружены в файл {file}')
        return

    data = send_get_request(BASE_URL + TEAM_META_INFORMATION_ENDPOINT.format(team_abbrs=','.join(TEAM_ABBRS)))
    df = pd.json_normalize(data)

    if df.empty:
        logger.warning(f'⚠️ Нет данных после нормализации метаданных команд')
        return

    curr_ts = get_current_utc_ts()
    df['updated_at'] = curr_ts.isoformat()
    s3_manager.save_to_parquet(df=df, key=f'{TEAM_META_PREFIX}/team_meta_{curr_ts.date()}.parquet')
    logger.info(f'✔️ Скачивание метаданных команд завершено')


def download_team_rosters(**context):
    """
    Скачивает текущие составы команд. Собирает множество из идентификаторов игроков и записывает его в XCom,
    чтобы в другой таске можно было получить информацию по каждому игроку
    """

    curr_ts = get_current_utc_ts()
    current_prefix = f'{TEAM_ROSTERS_PREFIX}/{curr_ts.date()}'
    player_ids = set()

    for team_abbr in TEAM_ABBRS:
        logger.info(f'📥 Начинается скачивание состава команды {team_abbr}')
        data = send_get_request(BASE_URL + TEAM_ROSTER_ENDPOINT.format(team_abbr=team_abbr, season=CURR_SEASON))
        df = pd.json_normalize(data)

        if not df.empty:
            player_ids.update(
                player['id']
                for player
                in chain(data['forwards'], data['defensemen'], data['goalies'])
            )

            df['updated_at'] = curr_ts.isoformat()
            s3_manager.save_to_parquet(df=df, key=f'{current_prefix}/{team_abbr}_roster.parquet')
            logger.info(f'✔️ Скачивание состава команды {team_abbr} завершено')
        else:
            logger.warning(f'⚠️ По составу команды {team_abbr} не получилось нормализовать данные')

    ti = context['ti']
    ti.xcom_push(key='current_roster_player_ids', value=player_ids)
    logger.info(f'#️⃣ Всего игроков в текущих составах: {len(player_ids)}')


def download_regular_season_schedule(**context):
    """
    Скачивает календарь игр сезона, разбитый по периодам в 7 дней. Если календарь относится
    к завершенному периоду (текущая дата больше даты завершения периода), то повторного
    сохранения и перезаписывания конкретного периода не происходит

    Также собирает множество из идентификаторов завершившихся за незакрытый период игр, по которым
    еще не была сохранена информация в S3. Записывает получившееся множество в XCom, чтобы в других тасках
    можно было получить подробную информацию по каждой игре
    """

    curr_ts = get_current_utc_ts()
    played_game_ids = set()

    for dt in pd.date_range(REGULAR_SEASON_START_DATE, REGULAR_SEASON_END_DATE, freq='7d'):
        start_period = dt.date()
        end_period = start_period + timedelta(days=6)

        filename = f'{start_period}-{end_period}.parquet'
        if s3_manager.file_exists(REGULAR_SEASON_SCHEDULE_PREFIX, filename) \
            and (curr_ts.date() - timedelta(days=1)) > end_period:
            logger.info(
                f'🗓️ Календарь игр с {start_period} по {end_period} уже сохранен и относится к прошедшему периоду')
            continue

        logger.info(f'🗓️ Начинается скачивание календаря игр с {start_period} по {end_period}')

        data = send_get_request(BASE_URL + SCHEDULE_BY_DATE_ENDPOINT.format(date=start_period))

        game_days = [day for day in data.get('gameWeek', []) if day.get('numberOfGames', 0)]
        if not game_days:
            logger.info(f'⚠️ В период с {start_period} по {end_period} нет игр. Переход к следующему периоду')
            continue

        data['gameWeek'] = game_days
        df = pd.json_normalize(data)

        if not df.empty:
            played_game_ids.update(
                game['id']
                for day in data['gameWeek']
                for game in day['games']
                if game['gameState'] == 'OFF'
                and game['gameType'] in VALID_GAME_TYPES
            )

            df['updated_at'] = curr_ts.isoformat()
            s3_manager.save_to_parquet(df=df, key=f'{REGULAR_SEASON_SCHEDULE_PREFIX}/{filename}')
            logger.info(f'✔️ Скачивание календаря игр с {start_period} по {end_period} завершено')
        else:
            logger.warning(f'⚠️ По календарю игр с {start_period} по {end_period} не получилось нормализовать данные')

    saved_played_game_ids = set(map(int, s3_manager.get_subprefix_last_segments(PLAYED_GAMES_PREFIX)))
    new_played_game_ids = played_game_ids - saved_played_game_ids

    ti = context['ti']
    ti.xcom_push(key='new_played_game_ids', value=new_played_game_ids)

    logger.info(f'#️⃣ Количество завершенных игр за инкремент: {len(new_played_game_ids)}')


def download_game_play_by_plays(**context):
    """
    Скачивает информацию обо всех игровых событиях, произошедших в рамках игр

    Собирает множество из идентификаторов игроков, заявленных на игру, и записывает его в XCom,
    чтобы в другой таске можно было получить информацию по каждому игроку
    Прим.: игрок, заявленный на матч, может отсутствовать в текущем составе команды. Поэтому и
    в этой функции требуется собирать идентификаторы игроков
    """

    ti = context['ti']
    new_played_game_ids = ti.xcom_pull(task_ids='get_regular_season_schedule', key='new_played_game_ids')
    logger.info(f'#️⃣ Завершенных игр получено: {len(new_played_game_ids)}')
    player_ids = set()

    for num, game_id in enumerate(new_played_game_ids, 1):
        logger.info(f'📥 {num}. Начинается скачивание данных игры {game_id}')

        data = send_get_request(BASE_URL + GAME_PLAY_BY_PLAY_ENDPOINT.format(game_id=game_id))
        df = pd.json_normalize(data)

        if not df.empty:
            player_ids.update(player['playerId'] for player in data['rosterSpots'])

            df['updated_at'] = get_current_utc_ts().isoformat()
            s3_manager.save_to_parquet(df=df, key=f'{PLAYED_GAMES_PREFIX}/{game_id}/play_by_play.parquet')
            logger.info(f'✔️ Скачивание данных игры {game_id} завершено')
        else:
            logger.warning(f'⚠️ По игре {game_id} не получилось нормализовать данные')

    ti.xcom_push(key='played_games_player_ids', value=player_ids)


def download_game_boxscores(**context):
    """
    Скачивает информацию о персональной статистике игроков за матч
    """

    ti = context['ti']
    new_played_game_ids = ti.xcom_pull(task_ids='get_regular_season_schedule', key='new_played_game_ids')
    logger.info(f'#️⃣ Завершенных игр получено: {len(new_played_game_ids)}')

    for num, game_id in enumerate(new_played_game_ids, 1):
        logger.info(f'📥 {num}. Начинается скачивание данных игры {game_id}')

        data = send_get_request(BASE_URL + GAME_BOXSCORE_ENDPOINT.format(game_id=game_id))
        df = pd.json_normalize(data)

        if not df.empty:
            df['updated_at'] = get_current_utc_ts().isoformat()
            s3_manager.save_to_parquet(df=df, key=f'{PLAYED_GAMES_PREFIX}/{game_id}/boxscore.parquet')
            logger.info(f'✔️ Скачивание данных игры {game_id} завершено')
        else:
            logger.warning(f'⚠️ По игре {game_id} не получилось нормализовать данные')


def download_game_stories(**context):
    """
    Скачивает результаты игр и командную статистику за матч

    Прим 1. Дополнительно обрабатываются полученные данные в summary.teamGameStats, поскольку они
    относятся к разным типам данных, из-за чего невозможно записать файл в parquet
    Прим 2. После нормализации данных в pandas-датафрейм массив summary.scoring сериализуется в
    json-строку, поскольку от игры к игре внутри может быть разная структура данных. Сериализация упрощает
    трансформацию данных на следующем этапе пайплайна - при чтении файлов в Spark и записи в БД
    """

    ti = context['ti']
    new_played_game_ids = ti.xcom_pull(task_ids='get_regular_season_schedule', key='new_played_game_ids')
    logger.info(f'#️⃣ Завершенных игр получено: {len(new_played_game_ids)}')

    for num, game_id in enumerate(new_played_game_ids, 1):
        logger.info(f'📥 {num}. Начинается скачивание данных игры {game_id}')

        data = send_get_request(BASE_URL + GAME_STORY_ENDPOINT.format(game_id=game_id))

        for item in data['summary']['teamGameStats']:
            item['awayValue'] = str(item['awayValue'])
            item['homeValue'] = str(item['homeValue'])

        df = pd.json_normalize(data)

        if not df.empty:
            df['updated_at'] = get_current_utc_ts().isoformat()
            df['summary.scoring'] = (
                df['summary.scoring']
                .apply(lambda x: json.dumps(x, ensure_ascii=False) if x is not None else None)
            )
            s3_manager.save_to_parquet(df=df, key=f'{PLAYED_GAMES_PREFIX}/{game_id}/game_story.parquet')
            logger.info(f'✔️ Скачивание данных игры {game_id} завершено')
        else:
            logger.warning(f'⚠️ По игре {game_id} не получилось нормализовать данные')


def download_players(**context):
    """
    Скачивает персональную информацию игроков
    """

    ti = context['ti']
    current_roster_player_ids = ti.xcom_pull(task_ids='get_team_rosters', key='current_roster_player_ids')
    played_games_player_ids = ti.xcom_pull(task_ids='get_played_game_play_by_plays', key='played_games_player_ids')

    unique_player_ids = current_roster_player_ids | played_games_player_ids
    logger.info(f'#️⃣ Игроков получено: {len(unique_player_ids)}')

    for num, player_id in enumerate(unique_player_ids, 1):
        logger.info(f'📥 {num}. Начинается скачивание данных игрока {player_id}')
        data = send_get_request(BASE_URL + PLAYER_INFO_ENDPOINT.format(player_id=player_id))

        df = pd.json_normalize(data)
        if not df.empty:
            df['updated_at'] = get_current_utc_ts().isoformat()
            s3_manager.save_to_parquet(df=df, key=f'{PLAYERS_PREFIX}/{player_id}.parquet')
            logger.info(f'✔️ Скачивание данных игрока {player_id} завершено')
        else:
            logger.warning(f'⚠️ По игроку {player_id} не получилось нормализовать данные')

    ti.xcom_push(key='total_players', value=len(unique_player_ids))


default_args = {
    'owner': 'Yegor U',
    'start_date': datetime(2025, 12, 1),
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'retry_exponential_backoff': True
}

with DAG(
    dag_id='nhl_api_to_s3',
    default_args=default_args,
    schedule_interval='0 7 * * *',
    catchup=False,
    description='Скачивание данных НХЛ',
    tags=['nhl']
) as dag:

    get_team_meta = PythonOperator(
        task_id='get_team_meta',
        python_callable=download_team_meta
    )

    get_team_rosters = PythonOperator(
        task_id='get_team_rosters',
        python_callable=download_team_rosters
    )

    get_regular_season_schedule = PythonOperator(
        task_id='get_regular_season_schedule',
        python_callable=download_regular_season_schedule
    )

    get_played_game_play_by_plays = PythonOperator(
        task_id='get_played_game_play_by_plays',
        python_callable=download_game_play_by_plays
    )

    get_played_game_boxscores = PythonOperator(
        task_id='get_played_game_boxscores',
        python_callable=download_game_boxscores
    )

    get_played_game_stories = PythonOperator(
        task_id='get_played_game_stories',
        python_callable=download_game_stories
    )

    get_players = PythonOperator(
        task_id='get_players',
        python_callable=download_players
    )

    trigger_nhl_s3_to_ch = TriggerDagRunOperator(
        task_id='trigger_nhl_s3_to_ch',
        trigger_dag_id='nhl_s3_to_ch'
    )

    check_telegram_connection = ShortCircuitOperator(
        task_id="check_telegram_connection",
        python_callable=lambda: telegram_conn_exists(TELEGRAM_CONN_ID)
    )

    send_telegram_message = TelegramOperator(
        task_id='send_telegram_message',
        telegram_conn_id=TELEGRAM_CONN_ID,
        text=(
            "🥂 DAG nhl_api_to_s3 отработал успешно\n"
            "Дата запуска: {{ macros.datetime.now().strftime('%d.%m.%Y') }}\n"
            "Игр загружено: {{ ti.xcom_pull(task_ids='get_regular_season_schedule', key='new_played_game_ids') | length }}\n"
            "Игроков обновлено: {{ ti.xcom_pull(task_ids='get_players', key='total_players') }}"
        )
    )

    get_team_meta >> [get_team_rosters, get_regular_season_schedule]
    get_regular_season_schedule >> get_played_game_boxscores >> get_played_game_stories >> get_played_game_play_by_plays
    [get_team_rosters, get_played_game_play_by_plays] >> get_players >> trigger_nhl_s3_to_ch >> \
    check_telegram_connection >> send_telegram_message