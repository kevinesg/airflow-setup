import requests
import pandas as pd
import pandas_gbq
from airflow.decorators import task
from google.cloud import storage, bigquery
from io import StringIO
from google.oauth2 import service_account




params = {
    'service_account_creds': './config/kevinesg-production-credentials.json',
    'project_id': 'kevinesg-production',
    'bucket_name': 'kevinesg-ph-news',
    'raw_folder_name': 'data/raw',
    'transformed_folder_name': 'data/transformed',
    'dataset': 'ph',
    'table': 'news',
    'min_date': '2023-06-01 00:00:00+08:00',
    'query_params': {
        'access_key': './config/mediastack_apikey.txt',
        'countries': 'ph',
        'language': 'en',
        'sort': 'published_desc',
        'limit': 100,
    }
} 


@task()
def api_to_gcs(
    service_account_creds:str=params['service_account_creds'],
    bucket_name:str=params['bucket_name'],
    raw_folder_name:str=params['raw_folder_name'],
    query_params:dict=params['query_params']
) -> None:

    with open(query_params['access_key'], 'r') as f:
        api_key:str = f.readline()
        query_params['access_key'] = api_key

    url:str = 'http://api.mediastack.com/v1/news'
    response = requests.get(url, params=query_params)
    data:list = response.json()['data']
    df:pd.DataFrame = pd.json_normalize(data)

    # Create a storage client
    client = storage.Client.from_service_account_json(service_account_creds)

    # Check if bucket already exists
    bucket = client.lookup_bucket(bucket_name)

    if bucket is None:
        bucket = client.create_bucket(bucket_name)
        print(f'Bucket {bucket.name} created.')

    blobs = bucket.list_blobs(prefix=f'{raw_folder_name}/')
    blobs_list = [blob.name for blob in blobs]

    if f'{raw_folder_name}/' not in blobs_list:

        bucket.blob(f'{raw_folder_name}/').upload_from_string('')
        print(f'{raw_folder_name}/ folder created.')

    latest_batch:str = 'latest_batch.csv'
    new_data:str = 'new_data.csv'
    if bucket is None:
        bucket = client.create_bucket(bucket_name)
        print(f'Bucket {bucket.name} created.')
        df_new:pd.DataFrame = df
    else:
        blob = bucket.blob(f'{raw_folder_name}/{latest_batch}')
        if blob.exists():
            data_blob = bucket.get_blob(f'{raw_folder_name}/{latest_batch}')
            data_str = data_blob.download_as_string()
            data_file = StringIO(data_str.decode('utf-8'))
            df_prev:pd.DataFrame = pd.read_csv(data_file)
            df_new:pd.DataFrame = df[~df['url'].isin(df_prev['url'])]
        else:
            df_new:pd.DataFrame = df
    
    bucket.blob(f'{raw_folder_name}/{latest_batch}').upload_from_string(df.to_csv(index=False), 'text/csv')
    bucket.blob(f'{raw_folder_name}/{new_data}').upload_from_string(df_new.to_csv(index=False), 'text/csv')

    return




@task()
def transform_raw_data(
    service_account_creds:str=params['service_account_creds'],
    bucket_name:str=params['bucket_name'],
    raw_folder_name:str=params['raw_folder_name'],
    transformed_folder_name:str=params['transformed_folder_name'],
    min_date:str=params['min_date']
) -> None:

    # Create a storage client
    client = storage.Client.from_service_account_json(service_account_creds)

    bucket = client.get_bucket(bucket_name)
    data_blob = bucket.get_blob(f'{raw_folder_name}/new_data.csv')
    data_str = data_blob.download_as_string()
    data_file = StringIO(data_str.decode('utf-8'))
    df = pd.read_csv(data_file)

    if df.shape[0] > 0:

        # data cleaning and filtering
        df.drop(columns=['language', 'country'], inplace=True)

        df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce')
        df['published_at'] = df['published_at'].apply(lambda x: convert_timezone(x, 'Asia/Manila'))
        df.dropna(subset=['published_at'], inplace=True)

        df = df[df['published_at'] >= min_date] # only relevant for the first couple of runs
    
    df_transformed_name:str = f'{transformed_folder_name}/df_transformed.csv'
    bucket.blob(df_transformed_name).upload_from_string(df.to_csv(index=False), 'text/csv')

    return




@task()
def ingest_to_gbq(
    service_account_creds:str=params['service_account_creds'],
    bucket_name:str=params['bucket_name'],
    transformed_folder_name:str=params['transformed_folder_name'],
    project_id:str=params['project_id'],
    dataset:str=params['dataset'],
    table:str=params['table']
) -> None:

    credentials = service_account.Credentials.from_service_account_file(service_account_creds)
    pandas_gbq.context.credentials = credentials

    # Create a storage client
    client = storage.Client.from_service_account_json(service_account_creds)

    bucket = client.get_bucket(bucket_name)
    data_blob = bucket.get_blob(f'{transformed_folder_name}/df_transformed.csv')
    data_str = data_blob.download_as_string()
    data_file = StringIO(data_str.decode('utf-8'))
    df = pd.read_csv(data_file)

    if df.shape[0] == 0:
        print('[INFO] No new rows to be ingested.')
        return

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

    print(f'[INFO] Ingesting new data to GBQ...')
    df.to_gbq(destination_table=f'{dataset}.{table}', project_id=project_id, if_exists='append')
    print(f'[INFO] Done ingesting new data to GBQ.')
    
    return      




def convert_timezone(ts, target_tz):
    if ts.tzinfo is None:
        return ts.tz_localize('UTC').tz_convert(target_tz)
    else:
        return ts.tz_convert(target_tz)