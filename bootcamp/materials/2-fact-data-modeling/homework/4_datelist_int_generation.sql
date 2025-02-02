-- Convert device_activity_datelist to datelist_int
create table user_datelist_int as
with date_bits as (
    select 
        user_id,
        browser_type,
        -- Convert each date in the array to a bit position
        -- Assuming we're tracking relative to a base date (e.g., '2024-01-01')
        unnest(device_activity_datelist) as activity_date,
        -- Calculate days since base date to determine bit position
        (date(unnest(device_activity_datelist)) - date '2024-01-01')::integer as day_offset
    from user_devices_cumulated
),
aggregated_bits as (
    select 
        user_id,
        activity_date,
        -- Set bits for each day of activity
        bit_or(1::bit(32) << day_offset) as datelist_int
    from date_bits
    where day_offset between 0 and 31  -- Ensure we're within 32-bit range
    group by user_id, activity_date
)
select 
    user_id,
    datelist_int,
    activity_date as date
from aggregated_bits;

-- Add primary key constraint
alter table user_datelist_int 
add primary key (user_id, date); 