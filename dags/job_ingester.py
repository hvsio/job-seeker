import json
import logging
from datetime import datetime, timedelta

import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from getters.cj_jobs import get_cj_jobs
from getters.muse_jobs import get_muse_jobs
from getters.reek_jobs import get_reek_jobs

logger = logging.getLogger('airflowLogger')

default_args = {
    'owner': 'Alicja',
    'retry': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='job_ingester',
    start_date=airflow.utils.dates.days_ago(2),
    schedule_interval='@daily',
    default_args=default_args,
    description='Daily job posting puller',
    template_searchpath='/tmp',
)

pull_cj = PythonOperator(
    task_id='pull_cj',
    python_callable=get_cj_jobs,
    provide_context=True,
    dag=dag,
)

pull_reek = PythonOperator(
    task_id='pull_reek',
    python_callable=get_reek_jobs,
    provide_context=True,
    dag=dag,
)

pull_muse = PythonOperator(
    task_id='pull_muse',
    python_callable=get_muse_jobs,
    provide_context=True,
    dag=dag,
)

write_cj_to_postgres = PostgresOperator(
    task_id='write_cj_to_postgres',
    postgres_conn_id='my_postgres',
    sql='cj_jobs.sql',
    dag=dag,
)

write_muse_to_postgres = PostgresOperator(
    task_id='write_muse_to_postgres',
    postgres_conn_id='my_postgres',
    sql='muse_jobs.sql',
    dag=dag,
)

write_reek_to_postgres = PostgresOperator(
    task_id='write_reek_to_postgres',
    postgres_conn_id='my_postgres',
    sql='reek_jobs.sql',
    dag=dag,
)

def _calculate_state():
    pg_hook = PostgresHook.get_hook('my_postgres')
    with open('/tmp/stats.sql', 'r') as f:
        sql_query = f.read()
    df = pg_hook.get_pandas_df(sql_query)
    logging.info(f'Stats from {datetime.today()}')
    logging.info(json.dumps(df.to_dict(), indent=4, default=str))


get_stats = PythonOperator(
    task_id='get_stats', python_callable=_calculate_state, dag=dag
)


pull_cj >> write_cj_to_postgres
pull_reek >> write_reek_to_postgres
pull_muse >> write_muse_to_postgres

[
    write_cj_to_postgres,
    write_reek_to_postgres,
    write_muse_to_postgres,
] >> get_stats
