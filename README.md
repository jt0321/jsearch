# JSearch - Real-Time Job Market Data Pipeline

This repository contains JSearch: a real-time data engineering pipeline that collects job postings from public APIs (RemoteOK, Adzuna), normalizes them, and streams them for downstream analytics.

## Architecture
![Workflow](./workflow.png)

1. **Collectors**: Python workers pull data from RemoteOK and Adzuna APIs.
2. **Kafka**: The raw data is published to a `jobs_raw` Kafka topic.
3. **Flink**: A PyFlink stream processing job consumes the Kafka topic and writes it into a Postgres sink.
4. **Storage**: PostgreSQL acts as the primary data warehouse, holding the normalized `jobs` table.
5. **Dashboard**: A Streamlit application built on top of the Postgres database to visualize market trends.

## Project Structure
- `src/collectors`: API collectors for RemoteOK and Adzuna
- `src/processor`: PyFlink streaming job (`flink_processor.py`)
- `src/dashboard`: Streamlit analytics application
- `docker-compose.yml`: Infrastructure (Kafka, Zookeeper, Flink, Postgres)
- `Dockerfile.flink`: Custom PyFlink image with Kafka and PostgreSQL JDBC connectors
- `.env.example`: sample environment variables

## Quickstart
1. Clone the repository and configure API keys:
   ```bash
   cp .env.example .env
   ```
2. Start the infrastructure:
   ```bash
   docker-compose up -d
   ```
3. Initialize the database schema:
   ```bash
   docker exec postgres psql -U admin -d jobs_db -c "CREATE TABLE IF NOT EXISTS jobs (title TEXT, company TEXT, location TEXT, remote BOOLEAN, skills TEXT[], salary TEXT, date_posted TEXT, source TEXT);"
   ```
4. Submit the PyFlink streaming job:
   ```bash
   docker exec jsearch-jobmanager-1 flink run -py src/processor/flink_processor.py
   ```
5. Run collectors to ingest data into Kafka:
   ```bash
   python -m src.main
   ```
