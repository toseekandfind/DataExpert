

SELECT 
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as column_count,
    (SELECT COUNT(*) FROM (SELECT 1 FROM ONLY pg_namespace n INNER JOIN ONLY pg_class c ON n.oid = c.relnamespace INNER JOIN ONLY pg_tables t2 ON c.relname = t2.tablename WHERE t2.tablename = t.table_name LIMIT 1) x) as row_count
FROM (
    SELECT tablename as table_name 
    FROM pg_tables 
    WHERE schemaname = 'public'
) t;