from datetime import datetime, timedelta
from airflow.decorators import dag




DATASET = 'finance'
DAG_NAME = f'etl__{DATASET}'
DAG_DESCRIPTION = 'ledger of personal income and expenses'
DAG_OWNER = 'kevinesg'
EMAIL = ['kevinlloydesguerra@gmail.com']
START_DATE = datetime(2023, 8, 30, 8)
CRON_SCHEDULE = '*/10 * * * *'
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


from tasks.task__finance_etl import gsheet_to_gcs, ingest_to_gbq
@dag(
    dag_id=DAG_NAME,
    start_date=START_DATE,
    schedule_interval=CRON_SCHEDULE,
    max_active_runs=MAX_ACTIVE_RUNS,
    concurrency=CONCURRENCY,
    default_args=default_args
)
def finance_etl() -> None:
    gsheet_to_gcs_task = gsheet_to_gcs()
    ingest_to_gbq_task = ingest_to_gbq()

    ingest_to_gbq_task.set_upstream(gsheet_to_gcs_task)

    return




finance_etl()