from airflow.exceptions import AirflowFailException
from airflow.hooks.http_hook import HttpHook
from getters.utils.validators import extract_location, unify_date_format
from getters.utils.formatters import query_formatter


def get_muse_jobs(**context):
    """Function ingesting job posting from Muse API
    and extracting them from Muse-specific JSON structure."""

    def extract_job_info(job: dict):
        job_title = job['name']
        company = job['company']['name']
        link = job['refs']['landing_page']
        type = job['type']
        region = extract_location(job['locations'][0]['name'])
        salary = None  # API does not provide this information
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
        hook = HttpHook('GET', 'muse')
        resp = hook.run()
        jobs_jsonified = resp.json()['results']
    except Exception as err:
        raise AirflowFailException(err)

    sql_insertion = query_formatter(jobs_jsonified, extract_job_info)

    with open('/tmp/muse_jobs.sql', 'w') as f:
        f.write(sql_insertion)
