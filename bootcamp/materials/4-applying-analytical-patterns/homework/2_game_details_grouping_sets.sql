with game_seasons as (
    select 
        gd.player_name,
        gd.team_abbreviation,
        g.season,
        sum(gd.pts) as total_points,
        count(distinct g.game_id) as games_played,
        sum(case when g.home_team_wins = 1 and gd.team_id = g.team_id_home 
             or g.home_team_wins = 0 and gd.team_id = g.team_id_away 
             then 1 else 0 end) as wins
    from game_details gd
    join games g on gd.game_id = g.game_id
    group by gd.player_name, gd.team_abbreviation, g.season
)
select 
    player_name,
    team_abbreviation,
    season,
    total_points,
    games_played,
    wins,
    grouping(player_name, team_abbreviation, season) as grouping_level
from game_seasons
group by grouping sets (
    (player_name, team_abbreviation), -- player and team aggregation
    (player_name, season),            -- player and season aggregation
    (team_abbreviation)               -- team only aggregation
)
order by 
    grouping_level,
    case when total_points is not null then total_points else 0 end desc; 