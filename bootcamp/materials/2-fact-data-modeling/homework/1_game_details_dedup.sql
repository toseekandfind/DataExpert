-- Create a deduplicated view of game_details
create table game_details_dedup as
with deduped as (
    select 
        game_id,
        team_id,
        team_abbreviation,
        team_city,
        player_id,
        player_name,
        nickname,
        start_position,
        comment,
        min,
        fgm,
        fga,
        fg_pct,
        fg3m,
        fg3a,
        fg3_pct,
        ftm,
        fta,
        ft_pct,
        oreb,
        dreb,
        reb,
        ast,
        stl,
        blk,
        "TO",
        pf,
        pts,
        plus_minus,
        -- Use row_number to identify duplicates
        row_number() over (
            partition by game_id, player_id  -- Natural key for uniqueness
            order by 
                -- Prefer records with more complete data
                (case when comment is null then 0 else 1 end) desc,
                (case when start_position is null then 0 else 1 end) desc,
                (case when min is null then 0 else 1 end) desc
        ) as rn
    from game_details
)
select 
    game_id,
    team_id,
    team_abbreviation,
    team_city,
    player_id,
    player_name,
    nickname,
    start_position,
    comment,
    min,
    fgm,
    fga,
    fg_pct,
    fg3m,
    fg3a,
    fg3_pct,
    ftm,
    fta,
    ft_pct,
    oreb,
    dreb,
    reb,
    ast,
    stl,
    blk,
    "TO",
    pf,
    pts,
    plus_minus
from deduped
where rn = 1;  -- Keep only the first record for each game_id, player_id combination

-- Add primary key constraint
alter table game_details_dedup 
add primary key (game_id, player_id); 