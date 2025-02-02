/*
Pipeline: NBA Player Career Tracking
Version: 1.0
Last Updated: 2024-02-01

OWNERSHIP AND SUPPORT
--------------------
Primary Owner: Data Engineering Team (data-eng@company.com)
Secondary Owner: Player Analytics Team (player-analytics@company.com)
On-call Rotation: Weekly rotation between DE team members (PagerDuty Schedule: NBA-analytics-oncall)

SERVICE LEVEL AGREEMENTS (SLAs)
------------------------------
Freshness: Daily updates by 6:00 AM EST
Accuracy: 100% accuracy required for player status changes
Completeness: All active and historical players must be tracked
Recovery Time Objective (RTO): 4 hours
Recovery Point Objective (RPO): 24 hours

DEPENDENCIES
-----------
Upstream:
- Player roster pipeline (roster_etl)
- Season schedule pipeline (season_schedule_etl)
- Historical player data pipeline (historical_players_etl)

Downstream:
- Player Career Dashboard
- Retirement Analysis Reports
- League Transition Reports

KNOWN ISSUES AND MITIGATIONS
---------------------------
1. Mid-season roster changes may cause delayed status updates
   Mitigation: Daily reconciliation with official NBA roster data

2. Historical data inconsistencies pre-2000
   Mitigation: Flagging system for low-confidence data points

3. Name variations for same player
   Mitigation: Player ID mapping table with aliases

MONITORING AND ALERTING
----------------------
Critical Alerts:
- Pipeline failure
- Missing player status updates
- Conflicting active status
- Data consistency violations

Warning Alerts:
- Unusual number of status changes
- Delayed source data
- Multiple status changes for same player

RECOVERY PROCEDURES
------------------
1. Pipeline Failure:
   a. Check upstream data completeness
   b. Verify player mapping consistency
   c. Run recovery script: /scripts/player_status_recovery.sh

2. Data Quality Issues:
   a. Execute validation suite: /scripts/validate_player_status.sql
   b. Compare with official NBA roster
   c. Manual review of discrepancies

3. Historical Data Issues:
   a. Check change history log
   b. Review source data quality
   c. Apply historical data fixes if needed

VALIDATION QUERIES
-----------------
1. Status Consistency Check:
   SELECT player_name, COUNT(DISTINCT is_active) as status_count
   FROM players
   WHERE current_season = (SELECT MAX(current_season) FROM players)
   GROUP BY player_name
   HAVING COUNT(DISTINCT is_active) > 1;

2. Transition Validation:
   SELECT p1.player_name
   FROM players p1
   JOIN players p2 ON p1.player_name = p2.player_name
   AND p1.current_season = p2.current_season - 1
   WHERE p1.is_active = false AND p2.is_active = true
   AND NOT EXISTS (
       SELECT 1 FROM player_transactions pt
       WHERE pt.player_name = p1.player_name
       AND pt.transaction_type = 'return_from_retirement'
       AND pt.season = p2.current_season
   );
*/

with player_status_changes as (
    -- Track player status changes between seasons
    -- Uses window function to compare current and previous season status
    select 
        player_name,
        current_season,
        is_active,
        lag(is_active) over (
            partition by player_name 
            order by current_season
        ) as previous_season_active
    from players
),
state_transitions as (
    -- Determine player state based on current and previous season status
    -- Implements the five possible states as per business requirements
    select 
        player_name,
        current_season,
        case
            when previous_season_active is null and is_active = true then 'New'
            when previous_season_active = true and is_active = false then 'Retired'
            when previous_season_active = true and is_active = true then 'Continued Playing'
            when previous_season_active = false and is_active = true then 'Returned from Retirement'
            when previous_season_active = false and is_active = false then 'Stayed Retired'
        end as player_state
    from player_status_changes
)
select 
    player_name,
    current_season,
    player_state
from state_transitions
order by 
    player_name,
    current_season; 