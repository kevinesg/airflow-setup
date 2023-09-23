from datetime import datetime, timedelta
from airflow.decorators import dag




DATASET = 'NYC_OpenData_FHV'
DAG_NAME = f'etl__{DATASET}'
DAG_DESCRIPTION = 'extracts data from NYC OpenData API to create NYC FHV database'
DAG_OWNER = 'kevinesg'
EMAIL = ['kevinlloydesguerra@gmail.com']
START_DATE = datetime(2023, 9, 16, 0)
CRON_SCHEDULE = '0 0 * * *'
RETRY_DELAY = timedelta(minutes=10)
RETRIES = 2
MAX_ACTIVE_RUNS = 1
CONCURRENCY = 1

default_args = {
    'description': DAG_DESCRIPTION,
    'owner': DAG_OWNER,
    'retries': RETRIES,
    'retry_delay': RETRY_DELAY,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': EMAIL
}


from tasks.task__nyc_opendata_fhv_etl import api_to_s3, transform_raw_data, ingest_to_gbq
@dag(
    dag_id=DAG_NAME,
    start_date=START_DATE,
    schedule_interval=CRON_SCHEDULE,
    max_active_runs=MAX_ACTIVE_RUNS,
    concurrency=CONCURRENCY,
    catchup=False,
    default_args=default_args
)
def fhv_etl() -> None:
    api_to_s3_task = api_to_s3()
    transform_raw_data_task = transform_raw_data()
    ingest_to_gbq_task = ingest_to_gbq()

    api_to_s3_task >> transform_raw_data_task >>ingest_to_gbq_task

    return




fhv_etl()