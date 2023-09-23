import requests
import json
import time
import pandas as pd
import pandas_gbq
from airflow.decorators import task
import boto3
from botocore.exceptions import ClientError
from google.cloud import bigquery
from io import StringIO
from google.oauth2 import service_account




params = {
    'aws_service_account_creds': './config/aws-kevinesg-rootkey.csv',
    'project_id': 'kevinesg-production',
    'aws_region': 'ap-southeast-1',
    'bucket_name': 'kevinesg-nyc-opendata-active-fhv',
    'raw_folder_name': 'data/raw',
    'transformed_folder_name': 'data/transformed',
    'gcp_service_account_creds': './config/kevinesg-production-credentials.json',
    'dataset': 'nyc_opendata',
    'table': 'fhv',
    'endpoint': 'https://data.cityofnewyork.us/resource/8wbx-tsch.json',
    'query_params': {
        '$limit': 10000,
        '$offset': 0
    }
} 
#workgroup-name.account-number.aws-region.redshift-serverless.amazonaws.com:port/dev
#default-workgroup.424488840630.ap-southeast-1.redshift-serverless.amazonaws.com:port/dev

@task()
def api_to_s3(
    aws_service_account_creds:str=params['aws_service_account_creds'],
    aws_region=params['aws_region'],
    bucket_name:str=params['bucket_name'],
    raw_folder_name:str=params['raw_folder_name'],
    endpoint:str=params['endpoint'],
    query_params:dict=params['query_params']
) -> None:

    # Load AWS access key ID and secret access key from your rootkey.csv
    print(f'[INFO] Creating s3 client...')
    rootkey:pd.DataFrame = pd.read_csv(aws_service_account_creds)
    # Create a session using your credentials
    session = boto3.Session(
        aws_access_key_id=rootkey['Access key ID'][0],
        aws_secret_access_key=rootkey['Secret access key'][0],
        region_name=aws_region
    )

    # Create an S3 client
    s3 = session.client('s3')
    print(f'[INFO] Done creating s3 client.')

    # check if bucket already exists
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f'[INFO] s3 bucket already exists.')
    except ClientError:
        # The bucket does not exist or you have no access.
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': aws_region
            }
        )
        print(f'Bucket {bucket_name} created.')

    # Check if raw folder exists
    result = s3.list_objects(Bucket=bucket_name, Prefix=f'{raw_folder_name}/')
    if 'Contents' not in result:
        s3.put_object(Bucket=bucket_name, Key=(f'{raw_folder_name}/'))
        print(f'{raw_folder_name}/ folder created.')

    while True:
        # Extract data from API
        chunk_size:int = query_params['$limit']
        offset:int = query_params['$offset']
        print(f'[INFO] Extracting chunk {offset // chunk_size + 1} from API...')
        response = requests.get(endpoint, params=query_params)
        data:list = response.json()
        
        if not data:
            break

        df:pd.DataFrame = pd.json_normalize(data)
        print(f'[INFO] Done extracting chunk {offset // chunk_size + 1}.')

        print(f'[INFO] Saving chunk {offset // chunk_size + 1} to s3 bucket...')
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=f'{raw_folder_name}/chunk_{offset // chunk_size + 1}.csv', Body=csv_buffer.getvalue())

        print(f'[INFO] Done saving chunk {offset // chunk_size + 1} to s3 bucket.')
        
        query_params.update({'$offset': query_params['$offset'] + query_params['$limit']})

        print(f'[INFO] Waiting 1 minute before extracting the next chunk...')
        time.sleep(60)
    
    print('Done saving complete data.')

    return





@task()
def transform_raw_data(
    aws_service_account_creds:str=params['aws_service_account_creds'],
    aws_region=params['aws_region'],
    bucket_name:str=params['bucket_name'],
    raw_folder_name:str=params['raw_folder_name'],
    transformed_folder_name:str=params['transformed_folder_name']
) -> None:

    # Load AWS access key ID and secret access key from your rootkey.csv
    print(f'[INFO] Creating s3 client...')
    rootkey:pd.DataFrame = pd.read_csv(aws_service_account_creds)
    # Create a session using your credentials
    session = boto3.Session(
        aws_access_key_id=rootkey['Access key ID'][0],
        aws_secret_access_key=rootkey['Secret access key'][0],
        region_name=aws_region
    )

    # Create an S3 client
    s3 = session.client('s3')
    print(f'[INFO] Done creating s3 client.')

    # Check if transformed folder exists
    result = s3.list_objects(Bucket=bucket_name, Prefix=f'{transformed_folder_name}/')
    if 'Contents' not in result:
        s3.put_object(Bucket=bucket_name, Key=(f'{transformed_folder_name}/'))
        print(f'{transformed_folder_name}/ folder created.')

    # List all CSV files in the folder
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=raw_folder_name)
    all_files = [file['Key'] for file in response['Contents'] if file['Key'].endswith('.csv')]

    for file in all_files:
        file_name = file.split('/')[-1]
        print(f'[INFO] Processing {file_name}...')
        # Get the CSV file from S3
        csv_obj = s3.get_object(Bucket=bucket_name, Key=file)
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_string))

        # Apply transformations to the chunk
        

        # Write the transformed chunk back to S3 (or to a new file)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=f'{transformed_folder_name}/transformed_{file_name}', Body=csv_buffer.getvalue())

    print('[INFO] Done with the transformation process.')

    return



@task()
def ingest_to_gbq(
    aws_service_account_creds:str=params['aws_service_account_creds'],
    aws_region=params['aws_region'],
    bucket_name:str=params['bucket_name'],
    transformed_folder_name:str=params['transformed_folder_name'],
    gcp_service_account_creds:str=params['gcp_service_account_creds'],
    project_id:str=params['project_id'],
    dataset:str=params['dataset'],
    table:str=params['table']
) -> None:

    # Load AWS access key ID and secret access key from your rootkey.csv
    print(f'[INFO] Creating s3 client...')
    rootkey:pd.DataFrame = pd.read_csv(aws_service_account_creds)
    
    # Create a session using your credentials
    session = boto3.Session(
        aws_access_key_id=rootkey['Access key ID'][0],
        aws_secret_access_key=rootkey['Secret access key'][0],
        region_name=aws_region
    )

    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)

    credentials = service_account.Credentials.from_service_account_file(gcp_service_account_creds)
    pandas_gbq.context.credentials = credentials

    # Create a bigquery client
    gbq_client = bigquery.Client.from_service_account_json(gcp_service_account_creds)
    datasets = list(gbq_client.list_datasets())
    dataset_list = [dataset.dataset_id for dataset in datasets]
    if dataset not in dataset_list:
        gbq_client.create_dataset(bigquery.Dataset(gbq_client.dataset(dataset)))
    
    tables = list(gbq_client.list_tables(dataset))
    table_list = [table.table_id for table in tables]
    if table not in table_list:
        gbq_client.create_table(bigquery.Table(f'{project_id}.{dataset}.{table}'))
    
    else:
    # Set up the query
        query = f"DELETE FROM `{dataset}.{table}` WHERE TRUE"

        # Run the query
        query_job = gbq_client.query(query)  # API request
        rows = query_job.result()  # Waits for query to finish

    print(f'[INFO] All old rows deleted from {dataset}.{table}.')
    
    for obj in bucket.objects.filter(Prefix=transformed_folder_name):
        # Check if the object is a CSV file
        if obj.key.endswith('.csv'):
            # Download the file from S3
            data_str = obj.get()['Body'].read().decode('utf-8')
            data_file = StringIO(data_str)
            
            # Read the file object into a pandas DataFrame
            df = pd.read_csv(data_file)

            print(f'[INFO] Ingesting {obj} to GBQ...')
            df.to_gbq(
                destination_table=f'{dataset}.{table}',
                project_id=project_id,
                if_exists='append'
            )
        
        print(f'[INFO] Waiting 1 minute before ingesting the next chunk...')
        time.sleep(60)

    print(f'[INFO] Done ingesting data to GBQ.')

    
    return