TEAM_META_LOCAL_DDL = '''
    CREATE TABLE IF NOT EXISTS {db}.team_meta_local ON CLUSTER '{{cluster}}'
    (
        team_id UInt32,
        team_abbrev String,
        team_name String,
        conference String,
        division String,
        updated_at DateTime  
    )
    ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{{cluster}}/{{shard}}/{db}/team_meta_local', '{{replica}}', updated_at)
    ORDER BY team_id
'''

TEAM_META_DISTRIBUTED_DDL = '''
    CREATE TABLE IF NOT EXISTS {db}.team_meta ON CLUSTER '{{cluster}}'
    (
        team_id UInt32,
        team_abbrev String,
        team_name String,
        conference String,
        division String,
        updated_at DateTime   
    )
    ENGINE = Distributed('{{cluster}}', {db}, team_meta_local, team_id)
'''

TEAM_ROSTERS_LOCAL_DDL = '''
    CREATE TABLE IF NOT EXISTS {db}.team_rosters_local ON CLUSTER '{{cluster}}'
    (
        team_id UInt32,
        team_abbrev String,
        team_name String,
        player_id UInt32,
        player_full_name String,
        position String,
        sweater_number UInt8,
        updated_at DateTime  
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{cluster}}/{{shard}}/{db}/team_rosters_local', '{{replica}}')
    ORDER BY (team_id, player_id)
'''

TEAM_ROSTERS_DISTRIBUTED_DDL = '''
    CREATE TABLE IF NOT EXISTS {db}.team_rosters ON CLUSTER '{{cluster}}'
    (
        team_id UInt32,
        team_abbrev String,
        team_name String,
        player_id UInt32,
        player_full_name String,
        position String,
        sweater_number UInt8,
        updated_at DateTime  
    )
    ENGINE = Distributed('{{cluster}}', {db}, team_rosters_local, team_id)
'''

TEAM_ROSTERS_TEMP_DDL = '''
    CREATE TABLE IF NOT EXISTS {db}.team_rosters_temp
    (
        team_id UInt32,
        team_abbrev String,
        team_name String,
        player_id UInt32,
        player_full_name String,
        position String,
        sweater_number UInt8,
        updated_at DateTime  
    )
    ENGINE = MergeTree()
    ORDER BY (team_id, player_id)
'''

TEAM_META_DATA_QUERY = '''
    SELECT
        team_id,
        team_abbrev,
        team_name
    FROM {db}.team_meta
    FINAL
'''

INSERT_INTO_TEAM_ROSTERS = '''
    INSERT INTO {db}.team_rosters
    SELECT *
    FROM {db}.team_rosters_temp
'''

SCHEDULE_LOCAL_DDL = '''
     CREATE TABLE IF NOT EXISTS {db}.schedule_local ON CLUSTER '{{cluster}}'
     (
         game_id UInt64,
         season UInt32,
         start_time DateTime,
         game_type UInt8,
         venue String,
         home_team_id UInt32,
         home_team_abbrev String,
         home_team_name String,
         away_team_id UInt32,
         away_team_abbrev String,
         away_team_name String,
         game_state String,
         game_schedule_state String,
         updated_at DateTime  
     )
     ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{{cluster}}/{{shard}}/{db}/schedule_local', '{{replica}}', updated_at)
     ORDER BY game_id
'''

SCHEDULE_DISTRIBUTED_DDL = '''
     CREATE TABLE IF NOT EXISTS {db}.schedule ON CLUSTER '{{cluster}}'
     (
         game_id UInt64,
         season UInt32,
         start_time DateTime,
         game_type UInt8,
         venue String,
         home_team_id UInt32,
         home_team_abbrev String,
         home_team_name String,
         away_team_id UInt32,
         away_team_abbrev String,
         away_team_name String,
         game_state String,
         game_schedule_state String,
         updated_at DateTime  
     )
     ENGINE = Distributed('{{cluster}}', {db}, schedule_local, game_id)
'''

PLAYERS_LOCAL_DDL = '''
    CREATE TABLE IF NOT EXISTS {db}.players_local ON CLUSTER '{{cluster}}'
    (
        player_id UInt32,
        is_active Bool,
        player_full_name String,
        birth_date Date,
        birth_city String,
        birth_country String,
        height_cm UInt8,
        weight_kg UInt8,
        headshot_img String,
        hero_img String,
        position String,
        sweater_number Nullable(UInt8),
        shoots_catches String,
        current_team_id Nullable(UInt32),
        current_team_abbrev Nullable(String),
        current_team_name Nullable(String),
        updated_at DateTime  
    )
    ENGINE = ReplicatedReplacingMergeTree(
        '/clickhouse/tables/{{cluster}}/{{shard}}/{db}/players_local',
        '{{replica}}',
        updated_at
    )
    ORDER BY player_id
 '''

PLAYERS_DISTRIBUTED_DDL = '''
    CREATE TABLE IF NOT EXISTS {db}.players ON CLUSTER '{{cluster}}'
    (
        player_id UInt32,
        is_active Bool,
        player_full_name String,
        birth_date Date,
        birth_city String,
        birth_country String,
        height_cm UInt8,
        weight_kg UInt8,
        headshot_img String,
        hero_img String,
        position String,
        sweater_number Nullable(UInt8),
        shoots_catches String,
        current_team_id Nullable(UInt32),
        current_team_abbrev Nullable(String),
        current_team_name Nullable(String),
        updated_at DateTime    
    )
    ENGINE = Distributed('{{cluster}}', {db}, players_local, player_id)
'''

GAME_RESULTS_LOCAL_DDL = '''
     CREATE TABLE IF NOT EXISTS {db}.game_results_local ON CLUSTER '{{cluster}}'
     (
        game_id UInt64,
        season UInt32,
        start_time DateTime,
        period_type String,
        home_team_score UInt8,
        away_team_score UInt8,
        home_team_id UInt32,
        home_team_abbrev String,
        home_team_name String,
        away_team_id UInt32,
        away_team_abbrev String,
        away_team_name String,
        three_stars_player_ids Array(UInt32),
        goals_goal_time Array(String),
        goals_home_score Array(UInt8),
        goals_away_score Array(UInt8),
        goals_strength Array(String),
        goals_scorer_player_id Array(UInt32),
        goals_assists_player_ids Array(Array(UInt32)),
        updated_at DateTime  
     )
     ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{cluster}}/{{shard}}/{db}/game_results_local', '{{replica}}')
     ORDER BY game_id
'''

GAME_RESULTS_DISTRIBUTED_DDL = '''
     CREATE TABLE IF NOT EXISTS {db}.game_results ON CLUSTER '{{cluster}}'
     (
        game_id UInt64,
        season UInt32,
        start_time DateTime,
        period_type String,
        home_team_score UInt8,
        away_team_score UInt8,
        home_team_id UInt32,
        home_team_abbrev String,
        home_team_name String,
        away_team_id UInt32,
        away_team_abbrev String,
        away_team_name String,
        three_stars_player_ids Array(UInt32),
        goals_goal_time Array(String),
        goals_home_score Array(UInt8),
        goals_away_score Array(UInt8),
        goals_strength Array(String),
        goals_scorer_player_id Array(UInt32),
        goals_assists_player_ids Array(Array(UInt32)),
        updated_at DateTime  
     )
     ENGINE = Distributed('{{cluster}}', {db}, game_results_local, game_id)
'''

TEAM_GAME_STATS_LOCAL_DDL = '''
     CREATE TABLE IF NOT EXISTS {db}.team_game_stats_local ON CLUSTER '{{cluster}}'
     (
        game_id UInt64,
        season UInt32,
        start_time DateTime,
        team_type String,
        team_id UInt32,
        team_abbrev String,
        team_name String,
        score UInt8,
        points UInt8,
        period_type String,
        shots_on_goal UInt8,
        shots_against UInt8,
        blocked_shots UInt8,
        faceoff_winning_pctg Float32,
        giveaways UInt8,
        takeaways UInt8,
        hits UInt8,
        penalty_minutes UInt16,
        power_play_goals UInt8,
        times_power_play UInt8,
        power_play_pctg Float32,
        penalty_kill UInt8,
        times_shorthand UInt8,
        penalty_kill_pctg Float32,
        updated_at DateTime  
     )
     ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{cluster}}/{{shard}}/{db}/team_game_stats_local', '{{replica}}')
     ORDER BY (game_id, team_id)
'''

TEAM_GAME_STATS_DISTRIBUTED_DDL = '''
     CREATE TABLE IF NOT EXISTS {db}.team_game_stats ON CLUSTER '{{cluster}}'
     (
        game_id UInt64,
        season UInt32,
        start_time DateTime,
        team_type String,
        team_id UInt32,
        team_abbrev String,
        team_name String,
        score UInt8,
        points UInt8,
        period_type String,
        shots_on_goal UInt8,
        shots_against UInt8,
        blocked_shots UInt8,
        faceoff_winning_pctg Float32,
        giveaways UInt8,
        takeaways UInt8,
        hits UInt8,
        penalty_minutes UInt16,
        power_play_goals UInt8,
        times_power_play UInt8,
        power_play_pctg Float32,
        penalty_kill UInt8,
        times_shorthand UInt8,
        penalty_kill_pctg Float32,
        updated_at DateTime  
     )
     ENGINE = Distributed('{{cluster}}', {db}, team_game_stats_local, game_id)
'''

SKATER_GAME_STATS_LOCAL_DDL = '''
     CREATE TABLE IF NOT EXISTS {db}.skater_game_stats_local ON CLUSTER '{{cluster}}'
     (
        game_id UInt64,
        season UInt32,
        start_time DateTime,
        team_type String,
        team_id UInt32,
        team_abbrev String,
        team_name String,
        player_id UInt32,
        player_name String,
        position String,
        sweater_number UInt8,
        goals UInt8,
        assists UInt8,
        points UInt8,
        plus_minus Int8,
        penalty_minutes UInt16,
        power_play_goals UInt8,
        shots UInt8,
        hits UInt8, 
        blocked_shots UInt8,
        faceoff_winning_pctg Float32,
        giveaways UInt8,
        takeaways UInt8,
        shifts UInt8,
        time_on_ice String,
        time_on_ice_seconds UInt16,
        updated_at DateTime  
     )
     ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{cluster}}/{{shard}}/{db}/skater_game_stats_local', '{{replica}}')
     ORDER BY (game_id, player_id)
'''

SKATER_GAME_STATS_DISTRIBUTED_DDL = '''
     CREATE TABLE IF NOT EXISTS {db}.skater_game_stats ON CLUSTER '{{cluster}}'
     (
        game_id UInt64,
        season UInt32,
        start_time DateTime,
        team_type String,
        team_id UInt32,
        team_abbrev String,
        team_name String,
        player_id UInt32,
        player_name String,
        position String,
        sweater_number UInt8,
        goals UInt8,
        assists UInt8,
        points UInt8,
        plus_minus Int8,
        penalty_minutes UInt16,
        power_play_goals UInt8,
        shots UInt8,
        hits UInt8, 
        blocked_shots UInt8,
        faceoff_winning_pctg Float32,
        giveaways UInt8,
        takeaways UInt8,
        shifts UInt8,
        time_on_ice String,
        time_on_ice_seconds UInt16,
        updated_at DateTime  
     )
     ENGINE = Distributed('{{cluster}}', {db}, skater_game_stats_local, game_id)
'''

GOALIE_GAME_STATS_LOCAL_DDL = '''
     CREATE TABLE IF NOT EXISTS {db}.goalie_game_stats_local ON CLUSTER '{{cluster}}'
     (
        game_id UInt64,
        season UInt32,
        start_time DateTime,
        team_type String,
        team_id UInt32,
        team_abbrev String,
        team_name String,
        player_id UInt32,
        player_name String,
        position String,
        sweater_number UInt8,
        starter Boolean,
        decision Nullable(String),
        saves UInt8,
        shots_against UInt8,
        save_pctg Nullable(Float32),
        goals_against UInt8,
        penalty_minutes UInt16,
        time_on_ice String,
        time_on_ice_seconds UInt16,
        updated_at DateTime  
     )
     ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{cluster}}/{{shard}}/{db}/goalie_game_stats_local', '{{replica}}')
     ORDER BY (game_id, player_id)
'''

GOALIE_GAME_STATS_DISTRIBUTED_DDL = '''
     CREATE TABLE IF NOT EXISTS {db}.goalie_game_stats ON CLUSTER '{{cluster}}'
     (
        game_id UInt64,
        season UInt32,
        start_time DateTime,
        team_type String,
        team_id UInt32,
        team_abbrev String,
        team_name String,
        player_id UInt32,
        player_name String,
        position String,
        sweater_number UInt8,
        starter Boolean,
        decision Nullable(String),
        saves UInt8,
        shots_against UInt8,
        save_pctg Nullable(Float32),
        goals_against UInt8,
        penalty_minutes UInt16,
        time_on_ice String,
        time_on_ice_seconds UInt16,
        updated_at DateTime  
     )
     ENGINE = Distributed('{{cluster}}', {db}, goalie_game_stats_local, game_id)
'''