-- DDL for hosts_cumulated table
create table hosts_cumulated (
    host text,
    host_activity_datelist date[],  -- Array of dates when host had activity
    last_updated_at timestamp,      -- Track when this record was last updated
    primary key (host)
);

-- Create an index on the array for better performance
create index idx_hosts_dates 
on hosts_cumulated using gin (host_activity_datelist);

-- Add comment to explain the table's purpose
comment on table hosts_cumulated is 
'Tracks activity dates for each host, storing dates in an array for efficient querying';

comment on column hosts_cumulated.host_activity_datelist is 
'Array of dates when the host experienced any activity'; 