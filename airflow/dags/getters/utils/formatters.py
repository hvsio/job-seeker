from typing import Callable

def prepare_sql_entry(job: dict) -> str:
    insert_query = f"\n ('{job['job_title']}', '{job['company'] or None}', '{job['link'] or None}', '{job['type'] or None}', '{job['region'] or None}', {job['salary'] or None}, TO_DATE('{job['date']}', 'DD/MM/YYYY'))"
    insert_query = insert_query.replace("'None'", 'NULL')
    insert_query = insert_query.replace('None', 'NULL')
    insert_query = insert_query.replace('nan', 'NULL')
    return insert_query

def query_formatter(jobs_jsonified: dict, extract_job_info: Callable) -> str:
    # add to sql statement
    sql = 'INSERT INTO job_data.jobs (job_title, company, link, type, region, salary, date) VALUES '
    for i, content in enumerate(jobs_jsonified):
        job = extract_job_info(content)
        insert_query = prepare_sql_entry(job)
        if i == len(jobs_jsonified) - 1:
            insert_query = f'{insert_query};'
        else:
            insert_query = f'{insert_query},'
        sql = sql + insert_query
    return sql