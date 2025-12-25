import pandas as pd
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    concat_ws,
    lit,
    pandas_udf,
    split,
    struct,
    when
)
from pyspark.sql.types import StringType
from unidecode import unidecode


@pandas_udf(StringType())
def unidecode_udf(column: pd.Series) -> pd.Series:
    """
    Декодирует языковые unicode-символы в ASCII-символы.
    Используется, чтобы заменить буквы с различными знаками над ними
    ASCII-аналогами в названиях команд, именах и городах рождения хоккеистов
    Например:
        Olli Määttä → Olli Maatta
        Montréal Canadiens → Montreal Canadiens
    """
    return column.apply(lambda x: None if x is None or pd.isna(x) else unidecode(x))


def normalize_roster_players_struct(x):
    """
    Принимает структуру и создает новую структуру только с необходимыми полями.
    Используется при трансформации составов команд
    """
    return struct(
        x.id.alias('player_id'),
        concat_ws(' ', x['firstName']['default'], x['lastName']['default']).alias('player_full_name'),
        x.positionCode.alias('position'),
        x.sweaterNumber.alias('sweater_number')
    )


def calculate_goal_game_time(period: Column, time_in_period: Column) -> Column:
    """
    Принимает колонку с номером периода и колонку со временем гола в рамках периода.
    Возвращает колонку со временем гола от начала матча. Время победного послематчевого буллита
    всегда равно 65:00 (60 минут основного времени + 5 минут овертайма)
    """
    time_parts = split(time_in_period, ':')
    minutes = time_parts.getItem(0).cast('int') + 20 * (period - 1)
    seconds = time_parts.getItem(1)
    goal_time = concat_ws(':', minutes, seconds)

    return when(period == 1, time_in_period).when(period == 5, lit('65:00')).otherwise(goal_time)


def calculate_seconds_on_ice(time_on_ice: Column) -> Column:
    """
    Принимает колонку с игровым временем игрока за матч, представленным в виде строки.
    Возвращает колонку с игровым временм игрока за матч в секундах
    """
    time_parts = split(time_on_ice, ':')
    minutes = time_parts.getItem(0).cast('int')
    seconds = time_parts.getItem(1).cast('int')
    return minutes * 60 + seconds


def normalize_skater_game_stats_struct(x):
    """
    Принимает структуру и создает новую структуру только с необходимыми полями.
    Используется при трансформации матчевой статистики полевых игроков
    """

    return struct(
        x.playerId.alias('player_id'),
        x['name']['default'].alias('player_name'),
        x.position,
        x.sweaterNumber.alias('sweater_number'),
        x.goals,
        x.assists,
        x.points,
        x.plusMinus.alias('plus_minus'),
        x.pim.alias('penalty_minutes'),
        x.powerPlayGoals.alias('power_play_goals'),
        x.sog.alias('shots'),
        x.hits,
        x.blockedShots.alias('blocked_shots'),
        x.faceoffWinningPctg.alias('faceoff_winning_pctg'),
        x.giveaways,
        x.takeaways,
        x.shifts,
        x.toi.alias('time_on_ice')
    )


def normalize_goalie_game_stats_struct(x):
    """
    Принимает структуру и создает новую структуру только с необходимыми полями.
    Используется при трансформации матчевой статистики голкиперов
    """

    return struct(
        x.playerId.alias('player_id'),
        x['name']['default'].alias('player_name'),
        x.position,
        x.sweaterNumber.alias('sweater_number'),
        x.starter,
        x.decision,
        x.saves,
        x.shotsAgainst.alias('shots_against'),
        x.savePctg.alias('save_pctg'),
        x.goalsAgainst.alias('goals_against'),
        x.pim.alias('penalty_minutes'),
        x.toi.alias('time_on_ice')
    )


def write_to_clickhouse(
   *,
   df: DataFrame,
   url: str,
   user: str,
   password: str,
   dbtable: str,
   batchsize: int = 1000,
   mode: str = 'append',
   coalesce: int = None
):
    """
    Записывает данные датафрейма df в Clickhouse с учетом переданных опций
    """

    df = df if coalesce is None else df.coalesce(coalesce)

    (
        df
        .write
        .format('jdbc')
        .option('jdbcCompliant', 'false')
        .option('isolationLevel', 'NONE')
        .option('driver', 'com.clickhouse.jdbc.ClickHouseDriver')
        .option('batchsize', batchsize)
        .option('url', url)
        .option('user', user)
        .option('password', password)
        .option('dbtable', dbtable)
        .mode(mode)
        .save()
    )
