-- Cumulative query to populate user_devices_cumulated
merge into user_devices_cumulated target
using (
    -- Get distinct user-browser-date combinations
    with daily_activity as (
        select 
            e.user_id,
            d.browser_type,
            date(e.event_time) as activity_date,
            max(e.event_time) as last_event_time
        from events e
        join devices d on e.device_id = d.device_id
        where date(e.event_time) <= :current_date
        group by 
            e.user_id,
            d.browser_type,
            date(e.event_time)
    ),
    -- Aggregate into arrays per user-browser
    aggregated as (
        select 
            user_id,
            browser_type,
            array_agg(distinct activity_date order by activity_date) as device_activity_datelist,
            max(last_event_time) as last_updated_at
        from daily_activity
        group by user_id, browser_type
    )
) source
on (
    target.user_id = source.user_id 
    and target.browser_type = source.browser_type
)
when matched then
    update set 
        device_activity_datelist = array(
            select distinct unnest(
                array_cat(target.device_activity_datelist, source.device_activity_datelist)
            ) order by 1
        ),
        last_updated_at = greatest(target.last_updated_at, source.last_updated_at)
when not matched then
    insert (user_id, browser_type, device_activity_datelist, last_updated_at)
    values (
        source.user_id, 
        source.browser_type, 
        source.device_activity_datelist,
        source.last_updated_at
    ); 