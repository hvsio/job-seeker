import requests
from airflow.exceptions import AirflowFailException
from getters.utils.validators import (
    extract_location,
    extract_numerical_salary,
    unify_date_format,
)
from getters.utils.formatters import query_formatter
from airflow.hooks.http_hook import HttpHook


def get_reek_jobs(**context):
    """Function ingesting job posting from Reek API
    and extracting them from Reek-specific JSON structure."""

    def extract_job_info(job: dict):
        job_title = job['jobTitle']
        company = job['employerName']
        link = job['jobUrl']
        type = None   # API does not provide that information
        region = extract_location(job['locationName'])
        salary = extract_numerical_salary(job['minimumSalary'])
        date = unify_date_format(str(context['execution_date']))

        return {
            'job_title': job_title,
            'company': company,
            'link': link,
            'type': type,
            'region': region,
            'salary': salary,
            'date': date,
        }

    # GET job postings
    try:
        hook = HttpHook('GET', 'reek')
        resp = hook.run()
        jobs_jsonified = resp.json()['results']
    except Exception as err:
        raise AirflowFailException(err)

    sql_insertion = query_formatter(jobs_jsonified, extract_job_info)

    with open('/tmp/reek_jobs.sql', 'w') as f:
        f.write(sql_insertion)
