-- Incremental query to populate hosts_cumulated
merge into hosts_cumulated target
using (
    -- Get distinct host-date combinations for the current date
    select 
        host,
        array_agg(distinct date(event_time) order by date(event_time)) as host_activity_datelist,
        max(event_time) as last_updated_at
    from events
    where date(event_time) = :current_date
    group by host
) source
on target.host = source.host
when matched then
    update set 
        -- Combine existing and new dates, removing duplicates
        host_activity_datelist = array(
            select distinct unnest(
                array_cat(target.host_activity_datelist, source.host_activity_datelist)
            ) order by 1
        ),
        last_updated_at = greatest(target.last_updated_at, source.last_updated_at)
when not matched then
    insert (host, host_activity_datelist, last_updated_at)
    values (
        source.host,
        source.host_activity_datelist,
        source.last_updated_at
    ); 