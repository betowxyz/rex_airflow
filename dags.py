import json
import kaggle
import pandas as pd

import pyarrow.parquet as pq
from pyarrow.parquet import ParquetFile

from google.cloud import storage
from google.cloud import bigquery
from google.auth import credentials
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

## constant
PATH = 'Z:\\Code\\stdy\\DE\\rex\\rex_challenge' # export - it cant be this path, only /home/airflow/rex/data
DATA_PATH = PATH + '\\' + 'data'

DATASET_FILE = DATA_PATH + '\\' + 'healthcare-dataset-stroke-data.csv'

PARQUET_NAME = 'healthcare-dataset-stroke-data.parquet'
PARQUET_FILE = DATA_PATH + '\\' + PARQUET_NAME
PARQUET_BUCKET = 'stroke-parquet'

SCHEMA_NAME = 'schema.json'
SCHEMA_FILE = DATA_PATH + '\\' + SCHEMA_NAME
SCHEMA_BUCKET = 'stroke-schema'

BQ_DATASET = 'rex_challenge'
BQ_TABLE = 'stroke_data'
BQ_DESTINATION = BQ_DATASET + '.' + BQ_TABLE

SECRET_JSON = PATH + '\\' + 'beto-cloud-4a0aaf7a011b.json'

def build_gcp_credentials(secret_json):
    '''
    Build the gcp credentials object for service account oauth
    Params:
        secret_json with the secrets, keys, and scopes
    '''

    with open(secret_json) as f:
        gcp_settings = json.load(f)

    project_id = gcp_settings['project_id']
    gcp_credentials = service_account.Credentials.from_service_account_info(gcp_settings)

    return gcp_credentials, project_id

def build_bq_client(gcp_credentials, project_id):
    '''
    Build the BigQuery client to perform requests
    Params:
        gcp_credentials: the object that ensures oauth
        project_id: the respective project of GCP
    '''

    bq_client = bigquery.Client(project = project_id, credentials = gcp_credentials)
    return bq_client

def build_storage_client(gcp_credentials, project_id):
    '''
    Build Storage client to perform requests to GCP buckets
    Params:
        gcp_credentials: the object that ensures oauth
        project_id: the respective project of GCP
    '''

    storage_client = storage.Client(project = project_id, credentials = gcp_credentials)
    return storage_client

def load_csv_data(csv_path_file):
    '''
    Load CSV data to pd.DataFrame object
    Params:
        csv_path_file: the path that the CSV file is
    '''

    df = pd.read_csv(csv_path_file)
    df = df.where(pd.notnull(df), None) ## just givining None to NaN values
    return df

def get_schema_from_parquet(parquet_path_file):
    '''
    Get schema from parquet file
    Params:
        parquet_path_file: the path that the parquet file is
    '''

    pfile = pq.read_table(parquet_path_file)
    schema = []

    rename_types = {
        'double': 'float',
        'int64': 'integer'
    }

    for item in pfile.schema:
        item_name = str(item.name)
        item_type = str(item.type)
        rename_type = rename_types.get(item_type)
        if(rename_type is not None):
            item_type = rename_type
        item_schema = {'name': item_name, 'type': item_type}
        schema.append(item_schema)

    return schema

def generate_bq_schema(schema):
    '''
    Generate BQ schema object (list of bigquery.Schemafield items)
    Params: 
        schema: dictionary schema extracted from parquet file
    '''

    bq_schema = []
    for item in schema:
        bq_schema.append(bigquery.SchemaField(item['name'], item['type'], mode="NULLABLE",))
    return bq_schema

def create_bq_table(bq_client, bq_schema, table_id):
    '''
    Create a bq table according to parameters
    Params:
        bq_client: BigQuery client that enable perform requests
        bq_schema: BigQuery schema from the table
        table_id: BigQuery table path
    '''

    table = bigquery.Table(table_id, schema = bq_schema)
    table = bq_client.create_table(table)

def save_dict_as_json(dict, file_name):
    '''
    Save dictionary as json file
    Params:
        dict: the dictionary to be save
        file_name: the file name / path
    '''

    with open(file_name, 'w') as fp:
        json.dump(dict, fp)

def read_json_to_dict(file_name):
    '''
    Read json file to dictionary
    Params: 
        file_name: the file name / path
    '''

    with open(file_name) as json_file:
        data = json.load(json_file)
    return data

def extract_numeric_columns(schema):
    '''
    Extract numeric columns from a given schema
    Params:
        schema: the given schema
    '''

    numeric = ['float', 'integer']
    return [item['name'] for item in schema if item['type'] in numeric]

def df_columns_to_numeric(df, columns):
    '''
    Transform DataFrame columns to numeric types (automatic with pandas)
    Params:
        columns: columns of DataFrame that will be converted to numeric
    '''

    for column in columns:
        df[column] = pd.to_numeric(df[column])

def load_to_bucket(storage_client, bucket_name, file_path, file_name):
    '''
    Load file to GCP bucket
    Params:
        storage_client: the Storage client that enables perform requests to GCP buckets
        bucket_name: destination bucket that the file will be inserted
        file_path: path of the file that will be sent to bucket
        file_name: the name of the file inside the bucket
    '''

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    with open(file_path, 'rb') as f:
        blob.upload_from_file(f)

def bq_table_exists(bq_client, table_id):
    '''
    Checks if the given table exists in the BigQuery project
    Params:
        bq_client: BigQuery client that enable perform requests
        table_id: BigQuery table path
    '''

    try:
        bq_client.get_table(table_id)
        return True
    except NotFound:
        return False

def get_data():
    '''
    DAG 1: Request stroke-prediction-dataset from kaggle (public dataset)
    '''

    kaggle.api.authenticate() ## authenticate in kaggle
    kaggle.api.dataset_download_files('fedesoriano/stroke-prediction-dataset', path=DATA_PATH, unzip=True) ## download the dataset in the DATA_PATH

def transform_to_parquet():
    '''
    Dag 2: Transform the data requested to parquet 
    '''

    df = load_csv_data(DATASET_FILE) ## loads the DATASET_FILE
    df.to_parquet(PARQUET_FILE) ## save the PARQUET_FILE

def load_schema_to_bucket():
    schema = get_schema_from_parquet(PARQUET_FILE) ## getting schema
    save_dict_as_json(schema, SCHEMA_FILE) ## saving schema to json

    gcp_credentials, project_id = build_gcp_credentials(SECRET_JSON) ## creating credentials object
    storage_client = build_storage_client(gcp_credentials, project_id) ## building storage client

    load_to_bucket(storage_client, SCHEMA_BUCKET, SCHEMA_FILE, SCHEMA_NAME)

def load_parquet_to_bucket():
    gcp_credentials, project_id = build_gcp_credentials(SECRET_JSON) ## creating credentials object
    storage_client = build_storage_client(gcp_credentials, project_id) ## building storage client

    load_to_bucket(storage_client, PARQUET_BUCKET, PARQUET_FILE, PARQUET_NAME)

def load_data_to_bq():
    gcp_credentials, project_id = build_gcp_credentials(SECRET_JSON)
    bq_client = build_bq_client(gcp_credentials, project_id)

    df = load_csv_data(DATASET_FILE) ## we can load from parquet too
    schema = read_json_to_dict(SCHEMA_FILE)
    bq_schema = generate_bq_schema(schema)
    table_id = project_id + '.' + BQ_DESTINATION

    if(not bq_table_exists(bq_client, table_id)): ## create table if don't exists
        create_bq_table(bq_client, bq_schema, table_id)

    numeric_columns = extract_numeric_columns(schema) ## prepare df to BQ
    df_columns_to_numeric(df, numeric_columns)

    df.to_gbq( ## send data to BQ
        destination_table = BQ_DESTINATION,
        project_id = project_id,
        credentials = gcp_credentials,
        if_exists = 'append')

get_data() ## 1 dag
# transform_to_parquet() ## 2 dag
# load_schema_to_bucket() ## 3 dag
# load_parquet_to_bucket() ## 4 dag
# load_data_to_bq() ## 5 dag

from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

## Hint: starting airflow
## $ airflow initdb
## $ airflow webserver

default_args = {
    'owner': 'lakshay',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id = 'get_data',
    default_args = default_args,
    schedule_interval = timedelta(days = 1),
    dagrun_timeout = timedelta(minutes = 60)
)

t1 = PythonOperator(
    task_id='get_data',
    python_callable= get_data,
    op_kwargs = None,
    dag=dag,
)

# t1