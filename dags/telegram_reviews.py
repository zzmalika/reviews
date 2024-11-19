from google_play_scraper import reviews_all, reviews, Sort
import time
import datetime as dt
import json
import os
import logging
import gc
import pandas as pd
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.exceptions import AirflowSkipException
from clickhouse_driver import Client


clickhouse_client = None
local_reviews_path = "/tmp/reviews_data/"
clickhouse_reviews_db = 'reviews_db'
telegram_reviews_table = 'telegram_table'
app_id = 'org.telegram.messenger'
# app_id = 'com.pikcloud.pikpak'

languages = ['ru', 'en']

def convert_datetime(obj):
    if isinstance(obj, dt.datetime):
        return obj.isoformat()
    

def get_clickhouse_client():
    global clickhouse_client

    if clickhouse_client is None:
        clickhouse_client = Client(host='clickhouse', user='default', password='default')
    return clickhouse_client


def check_db():

    client = get_clickhouse_client()
    client.execute(f'CREATE DATABASE IF NOT EXISTS {clickhouse_reviews_db}')
    logging.info('check_db finished')


def check_table():
    client = get_clickhouse_client()

    client.execute(f'''
                CREATE TABLE IF NOT EXISTS 
                   {clickhouse_reviews_db}.{telegram_reviews_table} (
                   event_date Date,
                   language String,
                   reviews_count Int32,
                   min_score Float32,
                   avg_score Float32,
                   max_score Float32,
                   insert_date Date,
                   insert_datetime DateTime
                   )
                ENGINE = ReplacingMergeTree(insert_datetime)
                PARTITION BY toYYYYMM(event_date)
                ORDER BY (event_date, language)
                ''')


def get_last_event_date(ti):
    last_event_date = 'EMPTY'
    client = get_clickhouse_client()

    res_exists = client.execute('SELECT COUNT(*) FROM reviews_db.telegram_table')
    if res_exists[0][0] == 0:
        last_event_date = 'EMPTY'
    else:
        res_max = client.execute('''
                    SELECT MAX(event_date) FROM reviews_db.telegram_table
                    ''')
        last_event_date = res_max[0][0] 
    
    print('----------------- last_event_date', last_event_date)

    ti.xcom_push(key='last_event_date', value=last_event_date)

    if last_event_date != 'EMPTY':
        return 'task_fetch_daily'
    else:
        return 'task_fetch_full'


def extract_daily_reviews(ti):
    print('start extracting DAILY reviews ...')    

    last_event_date = ti.xcom_pull(key='last_event_date')
    yesterday_utc = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).date()

    if not last_event_date:
        raise ValueError("No value found for 'last_event_date'")

    if not os.path.exists(local_reviews_path):
        os.makedirs(local_reviews_path)
    

    for language in languages:
        print('----------------- language', language)
        
        continuation_token = None
        all_reviews = []
        while True:
            batch_reviews, continuation_token = reviews(
                app_id,
                lang=language,
                sort=Sort.NEWEST,
                count=100,
                continuation_token=continuation_token
                # country='us',
                # sort=Sort.MOST_RELEVANT,
                # filter_score_with=5 
                )

            if not batch_reviews:
                break
            newest_review_dt = batch_reviews[0]['at'].date() 
            if newest_review_dt < last_event_date:
                    break

            all_reviews.extend(batch_reviews)

            if not continuation_token:
                break
        print('----------------- count all_reviews', len(all_reviews))

        filtered_reviews = [review for review in all_reviews if last_event_date < review["at"].date() < yesterday_utc]
        print('----------------- count filtered_reviews', len(filtered_reviews))

        save_to_local(filtered_reviews, f'{local_reviews_path}/{language}.json')
        print('----------------- save_to_local', f'{local_reviews_path}/{language}.json')


def save_to_local(reviews, path):
    print('----------------- starting save_to_local()')
    json_data = json.dumps(reviews, default=convert_datetime, indent=4, ensure_ascii=False)
    print('dumps ended')

    with open(path, 'w', encoding='utf-8') as f:
        f.write(json_data)

    del json_data
    del reviews
    gc.collect()


def extract_full_reviews():
    print('start extracting FULL reviews ...')    
    
    if not os.path.exists(local_reviews_path):
        os.makedirs(local_reviews_path)

    for language in languages:
        print('----------------- language', language)

        try:
            start_time = time.time()

            reviews = reviews_all(
                app_id,
                sleep_milliseconds=0,
                lang=language,
                # country='us',
                # sort=Sort.MOST_RELEVANT,
                # filter_score_with=5 
            )
            end_time = time.time()

        except Exception as e:
            raise AirflowSkipException("Google Play API is unavailable")


        print(f' ----------------- load time: {end_time - start_time} sec')

        print(' ----------------- count', len(reviews))
        print(f"Memory usage of reviews: {sys.getsizeof(reviews)} bytes")


        save_to_local(reviews, f'{local_reviews_path}/{language}.json')
        print(' ----------------- save_to_local', f'{local_reviews_path}/{language}.json')


        
def load_to_clickhouse(df):
    client = get_clickhouse_client()
    for column in df.select_dtypes(include=['datetime64']).columns:
        df[column] = df[column].apply(lambda x: x.to_pydatetime())


    insert_query = f"INSERT INTO {clickhouse_reviews_db}.{telegram_reviews_table} VALUES"
    data = df.to_dict(orient='records') 
    client.execute(insert_query, data)


def transform_and_load(ti):
    last_event_date = ti.xcom_pull(key='last_event_date')
    print(' ----------------- last_event_date', last_event_date)
    yesterday_utc = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).date()

    combined_df = []
    for language in languages:
        full_path = f'{local_reviews_path}/{language}.json'
        try:
            df = pd.read_json(full_path, lines=False, convert_dates=['at', 'repliedAt'])
        except Exception as e:
            print(e)

        df['event_date'] = df['at'].dt.date  
        
        if last_event_date != 'EMPTY':
            df = df[(df['at'].dt.date > last_event_date) & (df['at'].dt.date < yesterday_utc)]
        else:
            df = df[df['at'].dt.date < yesterday_utc]

        transformed_df = df.groupby(['event_date'], as_index=False).agg(
            reviews_count=('score', 'count'),
            min_score=('score', 'min'),
            avg_score=('score', 'mean'),
            max_score=('score', 'max')
        )
        transformed_df['language'] = language
        combined_df.append(transformed_df)

        os.remove(full_path)


    if combined_df:
        final_df = pd.concat(combined_df, ignore_index=True)

        final_df['insert_date'] = pd.Timestamp.now().date()
        final_df['insert_datetime'] = pd.Timestamp.now()

        load_to_clickhouse(final_df)
    else:
        print('No data in json')



dag_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': dt.datetime(2024, 11, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True,
    'retries': 2
}

dag = DAG(
    'telegram_reviews_pipeline',
    default_args=dag_args,
    schedule_interval='@daily',
    catchup=False

)

task_check_db = PythonOperator(
    task_id='check_db',
    python_callable=check_db,
    dag=dag
)

task_check_table= PythonOperator(
    task_id='check_table',
    python_callable=check_table,
    dag=dag
)

task_get_last_event_date = BranchPythonOperator(
    task_id='get_last_event_date',
    python_callable=get_last_event_date,
    dag=dag
)

task_fetch_full = PythonOperator(
    task_id='task_fetch_full',
    python_callable=extract_full_reviews,
    dag=dag
)

task_fetch_daily= PythonOperator(
    task_id='task_fetch_daily',
    python_callable=extract_daily_reviews,
    provide_context=True,  
    dag=dag
)

task_transform_and_load = PythonOperator(
    task_id='transform_and_load',
    python_callable=transform_and_load,
    provide_context=True,  

    dag=dag
)

task_join = EmptyOperator(
    task_id='join_branches',
    dag=dag,
    trigger_rule='one_success'
)

task_check_db >> task_check_table >> task_get_last_event_date 
task_get_last_event_date >> task_fetch_full >> task_join
task_get_last_event_date >> task_fetch_daily >> task_join
task_join >> task_transform_and_load


