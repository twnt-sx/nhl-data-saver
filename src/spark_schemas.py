from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType
)

CONFERENCES_SCHEMA = StructType([
    StructField('conference', StringType(), False),
    StructField('team_abbrev', StringType(), False)
])

DIVISIONS_SCHEMA = StructType([
    StructField('division', StringType(), False),
    StructField('team_abbrev', StringType(), False)
])

PLAYERS_SCHEMA = StructType([
    StructField('playerId', LongType(), True),
    StructField('isActive', BooleanType(), True),
    StructField('firstName.default', StringType(), True),
    StructField('lastName.default', StringType(), True),
    StructField('birthDate', StringType(), True),
    StructField('birthCity.default', StringType(), True),
    StructField('birthCountry', StringType(), True),
    StructField('heightInCentimeters', LongType(), True),
    StructField('weightInKilograms', LongType(), True),
    StructField('headshot', StringType(), True),
    StructField('heroImage', StringType(), True),
    StructField('position', StringType(), True),
    StructField('sweaterNumber', LongType(), True),
    StructField('shootsCatches', StringType(), True),
    StructField('currentTeamId', LongType(), True),
    StructField('currentTeamAbbrev', StringType(), True),
    StructField('fullTeamName.default', StringType(), True),
    StructField('updated_at', StringType(), True)
])

ASSISTS_SCHEMA = ArrayType(
    StructType([
        StructField('playerId', LongType(), True),
        StructField('firstName', StructType([StructField('default', StringType(), True)]), True),
        StructField('lastName', StructType([StructField('default', StringType(), True)]), True)
    ]),
    True
)

GOALS_SCHEMA = ArrayType(
    StructType([
        StructField('playerId', LongType(), True),
        StructField('strength', StringType(), True),
        StructField('timeInPeriod', StringType(), True),
        StructField('firstName', StructType([StructField('default', StringType(), True)]), True),
        StructField('lastName', StructType([StructField('default', StringType(), True)]), True),
        StructField('homeScore', LongType(), True),
        StructField('awayScore', LongType(), True),
        StructField('assists', ASSISTS_SCHEMA, True)
    ]),
    True
)

SCORING_SCHEMA = ArrayType(
    StructType([
        StructField('goals', GOALS_SCHEMA, True),
        StructField('periodDescriptor', StructType([StructField('number', LongType(), True)]), True)
    ])
)

THREE_STARS_SCHEMA = ArrayType(
    StructType([
        StructField('star', LongType(), True),
        StructField('playerId', LongType(), True)
    ]),
    True
)

TEAM_GAME_STATS_SCHEMA = ArrayType(
    StructType([
        StructField('awayValue', StringType(), True),
        StructField('category', StringType(), True),
        StructField('homeValue', StringType(), True)
    ]),
    True
)

GAME_STORY_SCHEMA = StructType([
    StructField('id', LongType(), True),
    StructField('season', LongType(), True),
    StructField('gameType', LongType(), True),
    StructField('startTimeUTC', StringType(), True),
    StructField('gameState', StringType(), True),
    StructField('gameScheduleState', StringType(), True),
    StructField('homeTeam.id', LongType(), True),
    StructField('homeTeam.abbrev', StringType(), True),
    StructField('homeTeam.placeName.default', StringType(), True),
    StructField('homeTeam.name.default', StringType(), True),
    StructField('homeTeam.score', LongType(), True),
    StructField('awayTeam.id', LongType(), True),
    StructField('awayTeam.abbrev', StringType(), True),
    StructField('awayTeam.placeName.default', StringType(), True),
    StructField('awayTeam.name.default', StringType(), True),
    StructField('awayTeam.score', LongType(), True),
    StructField('periodDescriptor.periodType', StringType(), True),
    StructField('summary.threeStars', THREE_STARS_SCHEMA, True),
    StructField('summary.teamGameStats', TEAM_GAME_STATS_SCHEMA, True),
    StructField('summary.scoring', StringType(), True),
    StructField('updated_at', StringType(), True)
])
