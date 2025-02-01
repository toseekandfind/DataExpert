with player_seasons_changes as (
    select 
        p1.player_name,
        p1.current_season,
        p1.is_active as current_active,
        lag(p1.is_active) over (
            partition by p1.player_name 
            order by p1.current_season
        ) as previous_active
    from players p1
),
state_changes as (
    select 
        player_name,
        current_season,
        case
            when previous_active is null and current_active = true then 'New'
            when previous_active = true and current_active = false then 'Retired'
            when previous_active = true and current_active = true then 'Continued Playing'
            when previous_active = false and current_active = true then 'Returned from Retirement'
            when previous_active = false and current_active = false then 'Stayed Retired'
        end as player_state
    from player_seasons_changes
)
select 
    player_name,
    current_season,
    player_state
from state_changes
order by player_name, current_season; 