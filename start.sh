cd source && poetry run python3 db/create_db.py
cd ..
docker-compose up airflow-init
docker-compose up
docker exec -it scheduler sh < airflow connections add 'my_postgres' --conn-uri 'postgresql://airflow:airflow@postgres:5432/jobs'
docker cp stats-utils/stats.sql worker:/tmp/stats.sql