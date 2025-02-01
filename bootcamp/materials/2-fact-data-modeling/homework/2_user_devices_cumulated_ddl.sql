-- DDL for user_devices_cumulated table
create table user_devices_cumulated (
    user_id bigint,
    browser_type text,
    device_activity_datelist date[],  -- Array of dates when user was active with this browser
    last_updated_at timestamp,        -- Track when this record was last updated
    primary key (user_id, browser_type)
);

-- Create an index on the array for better performance
create index idx_user_devices_dates 
on user_devices_cumulated using gin (device_activity_datelist);

-- Add comment to explain the table's purpose
comment on table user_devices_cumulated is 
'Tracks user activity dates by browser type, storing dates in an array for efficient querying';

comment on column user_devices_cumulated.device_activity_datelist is 
'Array of dates when the user was active with this browser type'; 