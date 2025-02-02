-- DDL for actors table
create table if not exists actors (
    actorid string,
    actor string,
    films array<struct<
        film: string,
        votes: int,
        rating: float,
        filmid: string
    >>,
    quality_class string,  -- star (>8), good (>7,<=8), average (>6,<=7), bad (<=6)
    is_active boolean,
    primary key (actorid)
); 