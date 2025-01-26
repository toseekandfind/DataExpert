-- DDL for actors_history_scd table (Type 2 SCD)
create table if not exists actors_history_scd (
    actorid string,
    actor string,
    quality_class string,  -- star (>8), good (>7,<=8), average (>6,<=7), bad (<=6)
    is_active boolean,
    start_date date,       -- When this version became active
    end_date date,         -- When this version became inactive (null if current)
    is_current boolean,    -- Flag to easily identify current version
    primary key (actorid, start_date)
); 