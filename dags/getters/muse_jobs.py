def get_muse_jobs(**context):
    import json
    import random
    from datetime import datetime

    import pandas as pd
    import requests

    day_index = datetime.now().day
    url = f'https://www.themuse.com/api/public/jobs?category=Computer%20and%20IT&category=Data%20Science&category=Software%20Engineer&category=Software%20Engineering&page={day_index}'

    def extract_job_info(job: dict):
        job_title = job['name']
        company = job['company']['name']
        link = job['refs']['landing_page']
        type = job['type']
        region = job['locations'][0]['name'].split(', ')[0]
        salary = random.randint(0, 7000)
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
    resp = requests.get(url)
    jobs_jsonified = json.loads(resp.content.decode())['results']

    # convert to dataframe
    df = pd.DataFrame()
    for content in jobs_jsonified:
        job = extract_job_info(content)
        df = pd.concat([df, pd.DataFrame.from_dict(job)])
    df.reset_index(drop=True, inplace=True)

    sql = 'INSERT INTO job_data.jobs (job_title, company, link, type, region, salary, date) VALUES '

    for i, entry in df.iterrows():
        insert_query = f"\n ('{entry['job_title']}', '{entry['company'] or None}', '{entry['link'] or None}', '{entry['type'] or None}', '{entry['region'] or None}', {entry['salary'] or None}, '{entry['date'] or None}')"
        insert_query = insert_query.replace("'None'", 'NULL')
        insert_query = insert_query.replace('None', 'NULL')
        insert_query = insert_query.replace('nan', 'NULL')

        if i == len(df) - 1:
            insert_query = f'{insert_query};'
        else:
            insert_query = f'{insert_query},'
        sql = sql + insert_query

    with open('/tmp/muse_jobs.sql', 'w') as f:
        f.write(sql)
