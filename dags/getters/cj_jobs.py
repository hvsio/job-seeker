

def get_cj_jobs(**context):
    import requests
    import pandas as pd
    import random
    from datetime import timedelta, datetime

    url = 'http://public.api.careerjet.net/search'

    def extract_job_info(job: dict):
        job_title = job['title']
        company = job['company']
        link = job['url']
        type = None
        region = job['locations'].split(',')[0]
        salary = job['salary'] if job['salary'] != 0 else random.randint(0, 7000)
        date= datetime.fromisoformat(str(context["execution_date"])).strftime('%d/%m/%Y')



        return {
            'job_title': [job_title],
            'company': [company],
            'link': [link],
            'type': [type],
            'region': [region],
            'salary': [salary],
            'date': [date]
        }


    params = {
        'location': '*',
        'keywords': 'data,engineering,software',
        'user_ip': '00.00.00.00',
        'user_agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:31.0) Gecko/20100101 Firefox/31.0',
        'pagesize': 50,
        'locale_code': 'da_DK'
    }

    # GET job postings
    resp = requests.get(url, params=params)
    jobs_jsonified = resp.json()['jobs']

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

    with open('/tmp/cj_jobs.sql', 'w') as f:
        f.write(sql)
