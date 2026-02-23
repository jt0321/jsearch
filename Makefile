.PHONY: up down build clean restart db-init flink-job collectors

# Run the entire pipeline in the background
up:
	docker-compose up -d

# Stop and remove all containers, networks
down:
	docker-compose down

# Rebuild all images and run
build:
	docker-compose up -d --build

# Stop, remove containers, and purge volumes/orphans
clean:
	docker-compose down -v --remove-orphans

# Restart the pipeline
restart: down up

# Manually initialize the Postgres schema
db-init:
	docker exec postgres psql -U admin -d jobs_db -c "CREATE TABLE IF NOT EXISTS jobs (title TEXT, company TEXT, location TEXT, remote BOOLEAN, skills TEXT[], salary TEXT, date_posted TEXT, source TEXT);"

# Manually submit the PyFlink job
flink-job:
	docker exec jsearch-jobmanager-1 flink run -py src/processor/flink_processor.py --detached

# Manually run Python collectors
collectors:
	docker exec jsearch-airflow-scheduler-1 python -m src.main
