import gspread as gs
import pandas as pd
import pandas_gbq
from airflow.decorators import task
from google.cloud import storage, bigquery
from google.oauth2 import service_account




params = {
    'service_account_creds' : './config/kevinesg-production-credentials.json',
    'gsheet_url' : 'https://docs.google.com/spreadsheets/d/1iOOfi8ZLbsWp2PsnxW0YctsFCZ9_fK6mZXDVnG993j8',
    'sheet_name' : 'data',
    'project_id' : 'kevinesg-production',
    'bucket_name' : 'kevinesg-finance',
    'raw_folder_name' : 'raw',
    'dataset': 'finance',
    'table': 'raw_data'
}


@task()
def gsheet_to_gcs(
    service_account_creds:str=params['service_account_creds'],
    gsheet_url:str=params['gsheet_url'],
    sheet_name:str=params['sheet_name'],
    bucket_name:str=params['bucket_name']
) -> None:

    # Create a storage client
    client = storage.Client.from_service_account_json(service_account_creds)

    # Check if bucket already exists
    bucket = client.lookup_bucket(bucket_name)
    if bucket is None:
        bucket = client.create_bucket(bucket_name)
        print(f'Bucket {bucket.name} created.')

    blobs = bucket.list_blobs()
    blobs_list = [blob.name for blob in blobs]

    # extract data from gsheet
    print(f'[INFO] Extracting raw data...')
    gc = gs.service_account(filename=service_account_creds)
    sh = gc.open_by_url(gsheet_url)
    ws = sh.worksheet(sheet_name)

    raw_df:pd.DataFrame = pd.DataFrame(ws.get_all_records())

    # save raw df to GCS
    blob = bucket.blob('raw_data.csv')
    blob.upload_from_string(raw_df.to_csv(index=False), 'text/csv')
    print(f'[INFO] Done saving raw data to GCS bucket.')

    return




@task()
def ingest_to_gbq(
    service_account_creds:str=params['service_account_creds'],
    bucket_name:str=params['bucket_name'],
    project_id:str=params['project_id'],
    dataset:str=params['dataset'],
    table:str=params['table']
) -> None:

    credentials = service_account.Credentials.from_service_account_file(service_account_creds)
    pandas_gbq.context.credentials = credentials

    # Create a bigquery client
    gbq_client = bigquery.Client.from_service_account_json(service_account_creds)
    datasets = list(gbq_client.list_datasets())
    dataset_list = [dataset.dataset_id for dataset in datasets]
    if dataset not in dataset_list:
        gbq_client.create_dataset(bigquery.Dataset(gbq_client.dataset(dataset)))
    
    tables = list(gbq_client.list_tables(dataset))
    table_list = [table.table_id for table in tables]
    if table not in table_list:
        gbq_client.create_table(bigquery.Table(f'{project_id}.{dataset}.{table}'))

    df = pd.read_csv(f'gs://{bucket_name}/raw_data.csv')
    print(f'[INFO] Ingesting raw data to GBQ...')
    df.to_gbq(
        destination_table=f'{dataset}.{table}',
        project_id=project_id,
        if_exists='replace'
    )
    
    return