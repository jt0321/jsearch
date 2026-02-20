import sys
import os
import json
import duckdb
from kafka import KafkaConsumer

# Add src to path to import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import KAFKA_BROKER, KAFKA_TOPIC

DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/jobs.duckdb'))

def init_db():
    conn = duckdb.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            title VARCHAR,
            company VARCHAR,
            location VARCHAR,
            remote BOOLEAN,
            skills VARCHAR[],
            salary VARCHAR,
            date_posted VARCHAR,
            source VARCHAR
        )
    """)
    return conn

def process_stream():
    conn = init_db()
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='job_processor_group'
    )

    print(f"Listening to Kafka topic: {KAFKA_TOPIC}")
    print(f"Writing to DuckDB at: {DB_PATH}")

    for message in consumer:
        job = message.value
        try:
            conn.execute("""
                INSERT INTO jobs (title, company, location, remote, skills, salary, date_posted, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job.get("title"),
                job.get("company"),
                job.get("location"),
                job.get("remote", False),
                job.get("skills", []),
                str(job.get("salary", "")),
                job.get("date_posted"),
                job.get("source")
            ))
            print(f"Inserted job: {job.get('title')} at {job.get('company')}")
        except Exception as e:
            print(f"Error inserting job: {e}")

if __name__ == "__main__":
    process_stream()
