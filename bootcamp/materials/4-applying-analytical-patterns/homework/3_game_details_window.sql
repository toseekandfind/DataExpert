-- Part 1: Find the most games a team has won in a 90 game stretch
with team_games as (
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
    where game_status_text = 'Final'
),
team_win_counts as (
    select 
        winning_team_id as team_id,
        game_date_est,
        count(*) over (
            partition by winning_team_id 
            order by game_date_est
            rows between 89 preceding and current row
        ) as wins_in_90_games
    from team_games
),
max_win_streaks as (
    select 
        team_id,
        max(wins_in_90_games) as max_wins_in_90_games
    from team_win_counts
    group by team_id
)
select 
    t.team_id,
    t.max_wins_in_90_games
from max_win_streaks t
order by max_wins_in_90_games desc
limit 1;

-- Part 2: Find LeBron James' streak of games scoring over 10 points
with lebron_games as (
    select 
        gd.game_id,
        g.game_date_est,
        gd.pts,
        case when gd.pts > 10 then 1 else 0 end as scored_over_10,
        case 
            when gd.pts <= 10 then 1 
            when lag(case when gd.pts > 10 then 0 else 1 end) over (order by g.game_date_est) = 1 then 1
            else 0 
        end as streak_breaker
    from game_details gd
    join games g on gd.game_id = g.game_id
    where gd.player_name = 'LeBron James'
    and g.game_status_text = 'Final'
),
streaks as (
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
    count(*) as streak_length
from streaks
group by streak_group
order by count(*) desc
limit 1; 