from datetime import datetime, timedelta
from airflow.decorators import dag




DATASET = 'USGS_earthquake'
DAG_NAME = f'etl__{DATASET}'
DAG_DESCRIPTION = 'extracts data from USGS API to create US earthquake database'
DAG_OWNER = 'kevinesg'
EMAIL = ['kevinlloydesguerra@gmail.com']
START_DATE = datetime(2023, 8, 22, 1)
CRON_SCHEDULE = '0 1 * * *'
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


from tasks.task__usgs_earthquake_etl import api_to_gcs, transform_raw_data, ingest_to_gbq
@dag(
    dag_id=DAG_NAME,
    start_date=START_DATE,
    schedule_interval=CRON_SCHEDULE,
    max_active_runs=MAX_ACTIVE_RUNS,
    concurrency=CONCURRENCY,
    default_args=default_args
)
def earthquake_etl() -> None:
    api_to_gcs_task = api_to_gcs()
    transform_raw_data_task = transform_raw_data()
    ingest_to_gbq_task = ingest_to_gbq()

    transform_raw_data_task.set_upstream(api_to_gcs_task)
    ingest_to_gbq_task.set_upstream(transform_raw_data_task)

    return




earthquake_etl()