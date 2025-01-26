-- Backfill query for actors_history_scd table
with yearly_status as (
    -- Calculate status for each actor for each year
    select 
        af.actorid,
        af.actor,
        af.year,
        -- Calculate quality class based on average rating for the year
        case 
            when avg(af.rating) > 8 then 'star'
            when avg(af.rating) > 7 then 'good'
            when avg(af.rating) > 6 then 'average'
            else 'bad'
        end as quality_class,
        -- Active if they have any films this year
        true as is_active
    from actor_films af
    group by af.actorid, af.actor, af.year
),
change_detection as (
    -- Detect when either quality_class or is_active status changes
    select 
        actorid,
        actor,
        year,
        quality_class,
        is_active,
        -- Mark start of new version when either status changes
        case when 
            lag(quality_class) over (partition by actorid order by year) != quality_class 
            or lag(is_active) over (partition by actorid order by year) != is_active
            or lag(actorid) over (order by actorid, year) is null  -- First record for actor
        then 1 else 0 end as is_new_version
    from yearly_status
),
version_boundaries as (
    -- Create version periods with start and end dates
    select 
        actorid,
        actor,
        quality_class,
        is_active,
        date(cast(year as string) || '-01-01') as start_date,
        date(cast(
            lead(year) over (partition by actorid order by year) - 1 
            as string) || '-12-31'
        ) as end_date,
        case when lead(year) over (partition by actorid order by year) is null 
            then true else false end as is_current
    from change_detection
    where is_new_version = 1
)
-- Insert all historical versions
insert into actors_history_scd (
    actorid, actor, quality_class, is_active, 
    start_date, end_date, is_current
)
select 
    actorid,
    actor,
    quality_class,
    is_active,
    start_date,
    -- Set end_date to null for current versions
    case when is_current then null else end_date end as end_date,
    is_current
from version_boundaries; 