from collections.abc import Callable


def prepare_sql_entry(job: dict) -> str:
    """Format a single job entry obtained from API into a
        stringified value enumeration describing SQL-insertable entry.

    Args:
        job (dict): dict-formatted job entry with attributes defined in Table definition.

    Returns:
        str: job postings values in a form of (value1, value2,...)
    """
    insert_query = (
        f"\n ('{job['job_title']}', "
        f"'{job['company'] or None}', "
        f"'{job['link'] or None}', "
        f"'{job['type'] or None}', "
        f"'{job['region'] or None}', "
        f"{job['salary'] or None}, "
        f"TO_DATE('{job['date']}', 'DD/MM/YYYY'))"
    )
    insert_query = insert_query.replace("'None'", 'NULL')
    insert_query = insert_query.replace('None', 'NULL')
    insert_query = insert_query.replace('nan', 'NULL')
    print(insert_query)
    return insert_query


def query_formatter(jobs_jsonified: dict, extract_job_info: Callable) -> str:
    """Constructs an SQL insertion query with all ingested jobs from single API.

    Args:
        jobs_jsonified (dict): Nested dict representing list of jobs
            (each job is defined in a dict)
        extract_job_info (Callable): API-specific extraction of pulled
            jobs' values adhering to table definiton

    Returns:
        str: _description_
    """
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
