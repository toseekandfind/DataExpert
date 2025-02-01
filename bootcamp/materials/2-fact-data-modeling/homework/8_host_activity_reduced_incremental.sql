-- Incremental query to populate host_activity_reduced
merge into host_activity_reduced target
using (
    with daily_metrics as (
        -- Calculate daily metrics for current date
        select 
            host,
            date(event_time) as event_date,
            count(*) as hit_count,
            count(distinct user_id) as unique_visitors,
            date_trunc('month', event_time)::date as month_start,
            extract(day from event_time)::integer - 1 as day_index,
            max(event_time) as last_event
        from events
        where date(event_time) = :current_date
        group by 
            host,
            date(event_time),
            date_trunc('month', event_time)::date
    )
    select 
        host,
        month_start as month,
        -- Create arrays with the day's metrics at the correct position
        array_fill(0, array[31]) ||  -- Initialize with zeros
            array_agg(hit_count) over (partition by host, month_start) as hit_array,
        array_fill(0, array[31]) ||  -- Initialize with zeros
            array_agg(unique_visitors) over (partition by host, month_start) as unique_visitors_array,
        max(last_event) as last_updated_at,
        max(day_index) as current_day_index
    from daily_metrics
) source
on (
    target.host = source.host 
    and target.month = source.month
)
when matched then
    update set 
        -- Update arrays at the specific day position
        hit_array = array_replace(
            target.hit_array, 
            target.hit_array[source.current_day_index + 1],
            source.hit_array[source.current_day_index + 1]
        ),
        unique_visitors_array = array_replace(
            target.unique_visitors_array,
            target.unique_visitors_array[source.current_day_index + 1],
            source.unique_visitors_array[source.current_day_index + 1]
        ),
        last_updated_at = greatest(target.last_updated_at, source.last_updated_at)
when not matched then
    insert (
        host,
        month,
        hit_array,
        unique_visitors_array,
        last_updated_at
    )
    values (
        source.host,
        source.month,
        source.hit_array,
        source.unique_visitors_array,
        source.last_updated_at
    ); 