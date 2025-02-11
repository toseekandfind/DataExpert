-- Calculate average events per session for each host
WITH host_stats AS (
    SELECT 
        host,
        COUNT(DISTINCT session_start || ip) as total_sessions,
        SUM(events_count) as total_events,
        ROUND(AVG(events_count)::numeric, 2) as avg_events_per_session
    FROM session_stats
    WHERE host IN (
        'zachwilson.techcreator.io',
        'zachwilson.tech',
        'lulu.techcreator.io'
    )
    GROUP BY host
)
SELECT 
    host,
    total_sessions,
    total_events,
    avg_events_per_session,
    RANK() OVER (ORDER BY avg_events_per_session DESC) as rank_by_engagement
FROM host_stats
ORDER BY avg_events_per_session DESC; 