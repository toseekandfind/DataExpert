-- Incremental update query for actors_history_scd table
with new_status as (
    -- Calculate new status for each actor from latest data
    select 
        af.actorid,
        af.actor,
        case 
            when avg(af.rating) > 8 then 'star'
            when avg(af.rating) > 7 then 'good'
            when avg(af.rating) > 6 then 'average'
            else 'bad'
        end as quality_class,
        true as is_active,
        date(:current_year || '-01-01') as effective_date
    from actor_films af
    where af.year = :current_year
    group by af.actorid, af.actor
),
current_status as (
    -- Get current version for each actor
    select *
    from actors_history_scd
    where is_current = true
),
status_changes as (
    -- Detect actors whose status has changed
    select 
        n.actorid,
        n.actor,
        n.quality_class as new_quality_class,
        n.is_active as new_is_active,
        c.quality_class as old_quality_class,
        c.is_active as old_is_active,
        n.effective_date,
        case 
            when c.actorid is null then 'NEW'
            when n.quality_class != c.quality_class 
                or n.is_active != c.is_active then 'CHANGED'
            else 'UNCHANGED'
        end as change_type
    from new_status n
    left join current_status c on n.actorid = c.actorid
)
-- Handle status changes
merge into actors_history_scd target
using (
    -- Generate records for changed and new actors
    select 
        actorid,
        actor,
        new_quality_class as quality_class,
        new_is_active as is_active,
        effective_date as start_date,
        null as end_date,
        true as is_current
    from status_changes
    where change_type in ('NEW', 'CHANGED')
) source
on target.actorid = source.actorid 
    and target.is_current = true
when matched then
    -- Update existing current record to become historical
    update set 
        end_date = dateadd(day, -1, source.start_date),
        is_current = false
when not matched then
    -- Insert new current record
    insert (actorid, actor, quality_class, is_active, start_date, end_date, is_current)
    values (
        source.actorid, 
        source.actor, 
        source.quality_class, 
        source.is_active, 
        source.start_date, 
        source.end_date, 
        source.is_current
    );

-- Insert new records for changed actors
insert into actors_history_scd (
    actorid, actor, quality_class, is_active, 
    start_date, end_date, is_current
)
select 
    actorid,
    actor,
    new_quality_class,
    new_is_active,
    effective_date,
    null,
    true
from status_changes
where change_type = 'CHANGED'; 