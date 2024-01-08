#!/bin/bash

# Postgresql connection parameters
AIRFLOW_USER="airflow"
AIRFLOW_PASSWORD="airflow"
POSTGRES_HOST="postgres"
POSTGRES_PORT="5432"
POSTGRES_DB="jobs"

# Start Airflow
docker-compose up airflow-init
docker-compose up -d

# Create table for job data
poetry run python3 source_db/create_db.py

# Add a connection to PostgreSQL for Airflow
CONNECTION_URI="postgresql://$AIRFLOW_USER:$AIRFLOW_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
docker exec -it scheduler sh -c "airflow connections add 'my_postgres' --conn-uri '$CONNECTION_URI'"

# Copy SQL file to the worker container
docker cp stats-utils/stats.sql worker:/tmp/stats.sql