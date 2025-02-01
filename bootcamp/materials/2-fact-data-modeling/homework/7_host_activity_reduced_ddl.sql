-- DDL for host_activity_reduced monthly fact table
create table host_activity_reduced (
    month date,                -- First day of the month
    host text,
    hit_array integer[],       -- Array of daily hit counts for the month
    unique_visitors_array integer[], -- Array of daily unique visitor counts
    last_updated_at timestamp,
    primary key (host, month)
);

-- Create indexes for better query performance
create index idx_host_activity_month 
on host_activity_reduced (month);

create index idx_host_activity_arrays 
on host_activity_reduced using gin (hit_array, unique_visitors_array);

-- Add table and column comments
comment on table host_activity_reduced is 
'Monthly reduced fact table tracking daily host activity metrics';

comment on column host_activity_reduced.hit_array is 
'Array of daily hit counts (position 0 = day 1, etc.)';

comment on column host_activity_reduced.unique_visitors_array is 
'Array of daily unique visitor counts (position 0 = day 1, etc.)'; 