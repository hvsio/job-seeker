#!/bin/bash
source .env
filepath=$(pwd)

# Start Airflow
docker compose --env-file ./.env up airflow-init
docker compose --env-file ./.env up -d

# Create table for job data
#poetry run python3 source_db/create_db.py

# Add a connection to PostgreSQL for Airflow
CONNECTION_URI="postgresql://$db_user:$db_pass@$db_host:$db_port/$tablename"

docker exec -it scheduler sh -c "airflow connections add 'postgres' --conn-uri '$CONNECTION_URI'"
docker exec -it scheduler sh -c "airflow connections add 'carrerjet' --conn-json '{\"conn_type\": \"http\",\"host\": \"http://public.api.careerjet.net/search\"}'"
docker exec -it scheduler sh -c "airflow connections add 'muse' --conn-json '{ \"conn_type\": \"http\", \"host\": \"https://www.themuse.com/api/public/jobs?category=Computer%20and%20IT&category=Data%20Science&category=Software%20Engineer&category=Software%20Engineering&page=0\"}'"
docker exec -it scheduler sh -c "airflow connections add 'reek' --conn-json '{\"conn_type\": \"http\", \"host\": \"https://www.reed.co.uk/api/1.0/search?keywords=engineer,data,software\", \"extra\": {\"Authorization\": \"Basic $REEK_APIKEY\"}}'"

# Copy SQL file to the worker container
docker cp ./sql/stats.sql worker:/tmp/stats.sql