import os

MINIO_ROOT_USER = os.getenv('MINIO_ROOT_USER')
MINIO_ROOT_PASSWORD = os.getenv('MINIO_ROOT_PASSWORD')
MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')

CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB')

CURR_SEASON = 20252026
REGULAR_SEASON_START_DATE = '2025-10-07'
REGULAR_SEASON_END_DATE = '2026-04-17'
VALID_GAME_TYPES = [2]
TEAM_ABBRS = ['ANA', 'BOS', 'BUF', 'CAR', 'CBJ', 'CGY', 'CHI', 'COL',
              'DAL', 'DET', 'EDM', 'FLA', 'LAK', 'MIN', 'MTL', 'NJD',
              'NSH', 'NYI', 'NYR', 'OTT', 'PHI', 'PIT', 'SEA', 'SJS',
              'STL', 'TBL', 'TOR', 'UTA', 'VAN', 'VGK', 'WPG', 'WSH']
CONFERENCES = {
    'Western': ['CHI', 'COL', 'DAL', 'MIN', 'NSH', 'STL', 'UTA', 'WPG',
                'ANA', 'CGY', 'EDM', 'LAK', 'SJS', 'SEA', 'VAN', 'VGK'],
    'Eastern': ['BOS', 'BUF', 'DET', 'FLA', 'MTL', 'OTT', 'TBL', 'TOR',
                'CAR', 'CBJ', 'NJD', 'NYI', 'NYR', 'PHI', 'PIT', 'WSH']
}
DIVISIONS = {
    'Pacific': ['ANA', 'CGY', 'EDM', 'LAK', 'SJS', 'SEA', 'VAN', 'VGK'],
    'Central': ['CHI', 'COL', 'DAL', 'MIN', 'NSH', 'STL', 'UTA', 'WPG'],
    'Atlantic': ['BOS', 'BUF', 'DET', 'FLA', 'MTL', 'OTT', 'TBL', 'TOR'],
    'Metropolitan': ['CAR', 'CBJ', 'NJD', 'NYI', 'NYR', 'PHI', 'PIT', 'WSH']
}

ROOT_PREFIX = 'api/nhl'
TEAM_META_PREFIX = f'{ROOT_PREFIX}/team_meta'
TEAM_ROSTERS_PREFIX = f'{ROOT_PREFIX}/team_rosters'
REGULAR_SEASON_SCHEDULE_PREFIX = f'{ROOT_PREFIX}/regular_season_schedule'
PLAYED_GAMES_PREFIX = f'{ROOT_PREFIX}/played_games'
PLAYERS_PREFIX = f'{ROOT_PREFIX}/players'

TELEGRAM_CONN_ID = 'telegram_default'
