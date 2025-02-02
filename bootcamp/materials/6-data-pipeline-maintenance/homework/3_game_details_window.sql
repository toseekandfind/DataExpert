/*
Pipeline: NBA Game Analytics
Version: 1.0
Last Updated: 2024-02-01

OWNERSHIP AND SUPPORT
--------------------
Primary Owner: Data Engineering Team (data-eng@company.com)
Secondary Owner: Sports Analytics Team (sports-analytics@company.com)
On-call Rotation: Weekly rotation between DE team members (PagerDuty Schedule: NBA-analytics-oncall)

SERVICE LEVEL AGREEMENTS (SLAs)
------------------------------
Freshness: Data must be updated within 6 hours of game completion
Accuracy: 100% accuracy required for game statistics
Completeness: No missing games or partial game data allowed
Recovery Time Objective (RTO): 2 hours
Recovery Point Objective (RPO): 24 hours

DEPENDENCIES
-----------
Upstream:
- Raw game data ingestion pipeline (games_raw_ingest)
- Player statistics pipeline (player_stats_etl)
- Team reference data pipeline (team_reference_etl)

Downstream:
- NBA Analytics Dashboard
- Team Performance Reports
- Player Achievement Tracking

KNOWN ISSUES AND MITIGATIONS
---------------------------
1. Game data delays during high-concurrency game nights
   Mitigation: Implemented retry logic with exponential backoff
   
2. Duplicate game entries possible during rescheduled games
   Mitigation: Deduplication logic based on game_id and game_date_est

3. Missing player statistics in rare cases
   Mitigation: Alert triggers if completeness check fails

MONITORING AND ALERTING
----------------------
Critical Alerts:
- Pipeline failure
- Data freshness > 6 hours
- Completeness check failure
- Duplicate game detection

Warning Alerts:
- Processing time > 30 minutes
- Unusual pattern in win streaks (potential data quality issue)

RECOVERY PROCEDURES
------------------
1. Pipeline Failure:
   a. Check error logs in Datadog
   b. Verify upstream dependencies
   c. Run recovery script: /scripts/nba_pipeline_recovery.sh
   
2. Data Quality Issues:
   a. Run validation queries: /scripts/validate_game_stats.sql
   b. Compare with official NBA stats API
   c. Trigger backfill if needed: /scripts/backfill_games.sh

3. Performance Issues:
   a. Check system resources
   b. Consider temporary scaling of compute resources
   c. Contact DevOps if persistence issues

VALIDATION QUERIES
-----------------
1. Completeness Check:
   SELECT COUNT(*) as missing_games
   FROM games g
   LEFT JOIN game_details gd ON g.game_id = gd.game_id
   WHERE gd.game_id IS NULL
   AND g.game_status_text = 'Final';

2. Freshness Check:
   SELECT MAX(game_date_est) as latest_game
   FROM games
   WHERE game_status_text = 'Final';
*/

-- Part 1: Find the most games a team has won in a 90 game stretch
with team_games as (
    -- Extract winning and losing teams from games table
    -- Handles both home and away team scenarios
    select 
        game_date_est,
        case 
            when home_team_wins = 1 then team_id_home
            else team_id_away
        end as winning_team_id,
        case 
            when home_team_wins = 1 then team_id_away
            else team_id_home
        end as losing_team_id
    from games
    where game_status_text = 'Final'  -- Only consider completed games
),
team_win_streaks as (
    -- Calculate rolling 90-game window for each team
    -- This helps identify the best performing periods
    select 
        winning_team_id as team_id,
        game_date_est,
        count(*) over (
            partition by winning_team_id 
            order by game_date_est
            rows between 89 preceding and current row
        ) as wins_in_90_games
    from team_games
)
select 
    team_id,
    max(wins_in_90_games) as max_wins_in_90_games
from team_win_streaks
group by team_id
order by max_wins_in_90_games desc
limit 1;

-- Part 2: Find LeBron James' streak of games scoring over 10 points
with lebron_games as (
    -- Get all LeBron's games and identify scoring patterns
    -- Includes streak breaking logic for accurate streak counting
    select 
        gd.game_id,
        g.game_date_est,
        gd.pts,
        case when gd.pts > 10 then 1 else 0 end as scored_over_10,
        case 
            when gd.pts <= 10 then 1 
            when lag(case when gd.pts > 10 then 0 else 1 end) 
                over (order by g.game_date_est) = 1 then 1
            else 0 
        end as streak_breaker
    from game_details gd
    join games g on gd.game_id = g.game_id
    where gd.player_name = 'LeBron James'
    and g.game_status_text = 'Final'
),
scoring_streaks as (
    -- Group consecutive games into streaks
    -- Uses window function to maintain streak grouping
    select 
        game_id,
        game_date_est,
        pts,
        sum(streak_breaker) over (order by game_date_est) as streak_group
    from lebron_games
    where scored_over_10 = 1
)
select 
    min(game_date_est) as streak_start,
    max(game_date_est) as streak_end,
    count(*) as consecutive_games_over_10_points
from scoring_streaks
group by streak_group
order by count(*) desc
limit 1; 