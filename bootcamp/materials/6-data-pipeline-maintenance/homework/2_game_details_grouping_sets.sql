/*
Pipeline: NBA Game Statistics Aggregation
Version: 1.0
Last Updated: 2024-02-01

OWNERSHIP AND SUPPORT
--------------------
Primary Owner: Data Engineering Team (data-eng@company.com)
Secondary Owner: Game Analytics Team (game-analytics@company.com)
On-call Rotation: Weekly rotation between DE team members (PagerDuty Schedule: NBA-analytics-oncall)

SERVICE LEVEL AGREEMENTS (SLAs)
------------------------------
Freshness: Updates within 2 hours of game completion
Accuracy: 100% accuracy required for game statistics
Completeness: All games and player statistics must be included
Recovery Time Objective (RTO): 2 hours
Recovery Point Objective (RPO): 12 hours

DEPENDENCIES
-----------
Upstream:
- Game details raw data pipeline (game_details_raw_etl)
- Team statistics pipeline (team_stats_etl)
- Player game logs pipeline (player_gamelogs_etl)

Downstream:
- Team Performance Dashboard
- Player Statistics Reports
- League Analytics Platform
- Broadcasting Statistics Feed

KNOWN ISSUES AND MITIGATIONS
---------------------------
1. Late game stat corrections
   Mitigation: Reprocessing mechanism for updated statistics

2. Missing player statistics in overtime periods
   Mitigation: Validation checks for complete game coverage

3. Duplicate statistics during rescheduled games
   Mitigation: Deduplication based on game_id and player_id

MONITORING AND ALERTING
----------------------
Critical Alerts:
- Pipeline failure
- Missing game statistics
- Data inconsistency between aggregation levels
- Processing delay > 2 hours

Warning Alerts:
- Unusual statistical outliers
- Partial game data
- High processing time

RECOVERY PROCEDURES
------------------
1. Pipeline Failure:
   a. Check data completeness in source tables
   b. Verify aggregation consistency
   c. Run recovery script: /scripts/game_stats_recovery.sh

2. Data Quality Issues:
   a. Execute validation suite: /scripts/validate_game_stats.sql
   b. Compare with official box scores
   c. Trigger reprocessing if needed

3. Performance Issues:
   a. Monitor query execution plans
   b. Check for resource contention
   c. Optimize aggregation strategy

VALIDATION QUERIES
-----------------
1. Aggregation Consistency Check:
   SELECT 
       g.game_id,
       ABS(g.pts_home - COALESCE(h.team_points, 0)) as home_point_diff,
       ABS(g.pts_away - COALESCE(a.team_points, 0)) as away_point_diff
   FROM games g
   LEFT JOIN (
       SELECT game_id, team_id, SUM(pts) as team_points
       FROM game_details
       GROUP BY game_id, team_id
   ) h ON g.game_id = h.game_id AND g.team_id_home = h.team_id
   LEFT JOIN (
       SELECT game_id, team_id, SUM(pts) as team_points
       FROM game_details
       GROUP BY game_id, team_id
   ) a ON g.game_id = a.game_id AND g.team_id_away = a.team_id
   WHERE h.team_points IS NULL 
      OR a.team_points IS NULL
      OR ABS(g.pts_home - h.team_points) > 0
      OR ABS(g.pts_away - a.team_points) > 0;

2. Player Statistics Completeness:
   SELECT gd.game_id, COUNT(DISTINCT gd.player_id) as players
   FROM game_details gd
   GROUP BY gd.game_id
   HAVING COUNT(DISTINCT gd.player_id) < 10;
*/

with game_stats as (
    -- Calculate comprehensive game statistics at player, team, and season level
    -- Includes points, games played, and wins for each combination
    select 
        gd.player_name,
        gd.team_abbreviation,
        g.season,
        sum(gd.pts) as total_points,
        count(distinct gd.game_id) as games_played,
        sum(case 
            when (g.home_team_wins = 1 and gd.team_id = g.team_id_home) 
              or (g.home_team_wins = 0 and gd.team_id = g.team_id_away)
            then 1 
            else 0 
        end) as wins
    from game_details gd
    join games g on gd.game_id = g.game_id
    group by 
        gd.player_name,
        gd.team_abbreviation,
        g.season
)
select 
    player_name,
    team_abbreviation,
    season,
    sum(total_points) as total_points,
    sum(games_played) as games_played,
    sum(wins) as total_wins,
    grouping(player_name, team_abbreviation, season) as grouping_level
from game_stats
group by grouping sets (
    (player_name, team_abbreviation),  -- Player performance by team
    (player_name, season),             -- Player performance by season
    (team_abbreviation)                -- Overall team performance
)
having sum(games_played) > 0  -- Exclude combinations with no games
order by 
    grouping_level,
    sum(total_points) desc nulls last; 