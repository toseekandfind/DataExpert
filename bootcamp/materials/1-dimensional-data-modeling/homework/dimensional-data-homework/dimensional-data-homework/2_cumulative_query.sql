-- Cumulative query to populate actors table year by year
merge into actors target
using (
    -- Get the latest data for each actor
    with actor_latest_data as (
        select 
            af.actorid,
            af.actor,
            -- Create array of film structs for all films up to current year
            array_agg(
                struct(
                    af.film as film,
                    af.votes as votes,
                    af.rating as rating,
                    af.filmid as filmid
                )
            ) as films,
            -- Calculate average rating for most recent year's films
            avg(case when af.year = max(af.year) over (partition by af.actorid) 
                then af.rating else null end) as recent_avg_rating,
            -- Check if active in current year
            max(af.year) = :current_year as is_active
        from actor_films af
        where af.year <= :current_year
        group by af.actorid, af.actor
    )
    select 
        actorid,
        actor,
        films,
        case 
            when recent_avg_rating > 8 then 'star'
            when recent_avg_rating > 7 then 'good'
            when recent_avg_rating > 6 then 'average'
            else 'bad'
        end as quality_class,
        is_active
    from actor_latest_data
) source
on target.actorid = source.actorid
when matched then
    update set 
        actor = source.actor,
        films = source.films,
        quality_class = source.quality_class,
        is_active = source.is_active
when not matched then
    insert (actorid, actor, films, quality_class, is_active)
    values (source.actorid, source.actor, source.films, source.quality_class, source.is_active); 