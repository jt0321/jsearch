import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def create_streaming_job():
    # 1. Setup Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=settings)

    # 2. Define Kafka Source Table
    # The topic 'jobs_raw' must exist. By default, cp-kafka will auto-create it if configured.
    t_env.execute_sql("""
        CREATE TEMPORARY TABLE kafka_source (
            `title` STRING,
            `company` STRING,
            `location` STRING,
            `remote` BOOLEAN,
            `skills` ARRAY<STRING>,
            `salary` STRING,
            `date_posted` STRING,
            `source` STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'jobs_raw',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink_processor_group',
            'scan.startup.mode' = 'earliest-offset',
            'value.format' = 'json',
            'value.json.fail-on-missing-field' = 'false',
            'value.json.ignore-parse-errors' = 'true'
        )
    """)

    # 3. Define PostgreSQL Sink Table
    # Flink JDBC connector handles inserting records into Postgres tables.
    # Postgres Arrays are supported in Flink 1.18 by casting appropriately or using standard mapping.
    t_env.execute_sql("""
        CREATE TABLE postgres_sink (
            `title` STRING,
            `company` STRING,
            `location` STRING,
            `remote` BOOLEAN,
            `skills` STRING,
            `salary` STRING,
            `date_posted` STRING,
            `source` STRING
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/jobs_db',
            'table-name' = 'jobs',
            'username' = 'admin',
            'password' = 'password',
            'driver' = 'org.postgresql.Driver',
            'sink.buffer-flush.max-rows' = '1'
        )
    """)

    # 4. Execute Insertion Pipeline
    print("Submitting Flink Job: Kafka -> PostgreSQL...")
    t_env.execute_sql("""
        INSERT INTO postgres_sink
        SELECT 
            title, 
            company, 
            location, 
            remote, 
            CAST(skills AS STRING), 
            salary, 
            date_posted, 
            source 
        FROM kafka_source
    """)

if __name__ == '__main__':
    create_streaming_job()
