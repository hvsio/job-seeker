from airflow.exceptions import AirflowFailException
from airflow.hooks.http_hook import HttpHook
from getters.utils.validators import (
    extract_location,
    extract_numerical_salary,
    unify_date_format,
)
from getters.utils.formatters import query_formatter


def get_cj_jobs(**context):
    """Function ingesting job posting from CarrerJet API and
    extracting them from CarrerJet-specific JSON structure."""

    def extract_job_info(job: dict):
        dkk_to_euro = 0.13

        job_title = job['title']
        company = job['company']
        link = job['url']
        type = None  # API does not provide that information
        region = extract_location(job['locations'])
        salary = extract_numerical_salary(job['salary'], dkk_to_euro)
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
        hook = HttpHook('GET', 'carrerjet')
        resp = hook.run(
            data={
                'location': '*',
                'keywords': 'data,engineering,software',
                'user_ip': '00.00.00.00',
                'pagesize': 50,
                'locale_code': 'da_DK',
            }
        )
        jobs_jsonified = resp.json()['jobs']
    except Exception as err:
        raise AirflowFailException(err)

    sql_insertion = query_formatter(jobs_jsonified, extract_job_info)

    with open('/tmp/cj_jobs.sql', 'w') as f:
        f.write(sql_insertion)
