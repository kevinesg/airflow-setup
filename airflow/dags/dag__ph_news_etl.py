from datetime import datetime, timedelta
from airflow.decorators import dag




DATASET = 'PH_news'
DAG_NAME = f'etl__{DATASET}'
DAG_DESCRIPTION = 'extracts data from mediastack API to create a PH news database'
DAG_OWNER = 'kevinesg'
EMAIL = ['kevinlloydesguerra@gmail.com']
START_DATE = datetime(2023, 9, 1, 6)
CRON_SCHEDULE = '0 */2 * * *'
RETRY_DELAY = timedelta(minutes=2)
RETRIES = 5
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


from tasks.task__ph_news_etl import api_to_gcs, transform_raw_data, ingest_to_gbq
@dag(
    dag_id=DAG_NAME,
    start_date=START_DATE,
    schedule_interval=CRON_SCHEDULE,
    max_active_runs=MAX_ACTIVE_RUNS,
    concurrency=CONCURRENCY,
    catchup=False,
    default_args=default_args
)
def ph_news_etl() -> None:
    api_to_gcs_task = api_to_gcs()
    transform_raw_data_task = transform_raw_data()
    ingest_to_gbq_task = ingest_to_gbq()

    api_to_gcs_task >> transform_raw_data_task >> ingest_to_gbq_task

    return




ph_news_etl()