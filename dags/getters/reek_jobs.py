def get_reek_jobs(**context):
    import random
    from datetime import datetime

    import pandas as pd
    import requests

    REEK_APIKEY = '[PLACEHOLDER]'
    url = (
        'https://www.reed.co.uk/api/1.0/search?keywords=engineer,data,software'
    )

    def extract_job_info(job: dict):
        job_title = job['jobTitle']
        company = job['employerName']
        link = job['jobUrl']
        type = None
        region = job['locationName']
        salary = (
            job['minimumSalary']
            if job['minimumSalary'] != 0
            else random.randint(3000, 7000)
        )
        date = datetime.fromisoformat(str(context['execution_date'])).strftime(
            '%d/%m/%Y'
        )

        return {
            'job_title': [job_title],
            'company': [company],
            'link': [link],
            'type': [type],
            'region': [region],
            'salary': [salary],
            'date': [date],
        }

    # GET job postings
    headers = {'Authorization': f'Basic {REEK_APIKEY}'}

    resp = requests.request('GET', url, headers=headers)
    jobs_jsonified = resp.json()['results']

    # convert to dataframe
    df = pd.DataFrame()
    for content in jobs_jsonified:
        job = extract_job_info(content)
        df = pd.concat([df, pd.DataFrame.from_dict(job)])
    df.reset_index(drop=True, inplace=True)

    sql = 'INSERT INTO job_data.jobs (job_title, company, link, type, region, salary, date) VALUES '

    for i, entry in df.iterrows():
        insert_query = f"\n ('{entry['job_title']}', '{entry['company'] or None}', '{entry['link'] or None}', '{entry['type'] or None}', '{entry['region'] or None}', {entry['salary'] or None}, TO_DATE('{entry['date']}', 'DD/MM/YYYY'))"
        insert_query = insert_query.replace("'None'", 'NULL')
        insert_query = insert_query.replace('None', 'NULL')
        insert_query = insert_query.replace('nan', 'NULL')

        if i == len(df) - 1:
            insert_query = f'{insert_query};'
        else:
            insert_query = f'{insert_query},'
        sql = sql + insert_query

    with open('/tmp/reek_jobs.sql', 'w') as f:
        f.write(sql)
