from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    EnvironmentSettings,
    DataTypes,
    TableEnvironment,
    StreamTableEnvironment
)
import os
import json

def create_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "events"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            url VARCHAR,
            referrer VARCHAR,
            user_agent VARCHAR,
            host VARCHAR,
            ip VARCHAR,
            headers VARCHAR,
            event_time VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECONDS
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
        """
    print(source_ddl)
    t_env.execute_sql(source_ddl)
    return table_name

def create_session_stats_sink_postgres(t_env):
    table_name = 'session_stats'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            host VARCHAR,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            ip VARCHAR,
            events_count BIGINT,
            PRIMARY KEY (host, session_start, ip) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def session_analysis():
    print('Starting Session Analysis Job!')
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    print('got streaming environment')
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    try:
        # Create source and sink tables
        source_table = create_events_source_kafka(t_env)
        session_stats_sink = create_session_stats_sink_postgres(t_env)
        
        # Create session windows and calculate statistics
        session_query = f"""
            INSERT INTO {session_stats_sink}
            SELECT 
                host,
                window_start as session_start,
                window_end as session_end,
                ip,
                COUNT(*) as events_count
            FROM TABLE(
                SESSION(
                    TABLE {source_table},
                    DESCRIPTOR(event_timestamp),
                    INTERVAL '5' MINUTES
                )
            )
            GROUP BY 
                window_start,
                window_end,
                host,
                ip
        """
        
        print('Executing session analysis query...')
        t_env.execute_sql(session_query).wait()
        
    except Exception as e:
        print("Session analysis job failed:", str(e))

if __name__ == '__main__':
    session_analysis() 