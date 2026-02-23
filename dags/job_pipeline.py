from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'jsearch_pipeline',
    default_args=default_args,
    description='JSearch Data Pipeline Orchestration',
    schedule_interval=timedelta(minutes=30),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['jsearch'],
) as dag:

    init_db = PostgresOperator(
        task_id='init_postgres_schema',
        postgres_conn_id='jsearch_postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS jobs (
                title TEXT,
                company TEXT,
                location TEXT,
                remote BOOLEAN,
                skills TEXT[],
                salary TEXT,
                date_posted TEXT,
                source TEXT
            );
        """
    )
    
    # Check if the PyFlink job is already running to avoid submitting it multiple times
    # If it's not running, submit it. Flilnk JobManager might already be executing it.
    submit_flink_job = BashOperator(
        task_id='submit_flink_job',
        bash_command='''
            RUNNING=$(docker exec jsearch-jobmanager-1 flink list | grep "flink_processor" || true)
            if [ -z "$RUNNING" ]; then
                echo "Submitting PyFlink job..."
                docker exec jsearch-jobmanager-1 flink run -py src/processor/flink_processor.py --detached
            else
                echo "Job is already running."
            fi
        '''
    )

    run_collectors = BashOperator(
        task_id='run_python_collectors',
        bash_command='cd /opt/airflow && python -m src.main',
    )

    init_db >> submit_flink_job >> run_collectors
