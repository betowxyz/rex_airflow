import json

import kaggle

import pandas as pd

import pyarrow.parquet as pq

from pyarrow.parquet import ParquetFile

# from google.cloud import bigquery

# from google.oauth2 import service_account

## Hint: starting airflow
## $ airflow initdb
## $ airflow webserver

## constant
DATA_PATH = 'Z:\\Code\\stdy\\DE\\rex\\rex_challenge\\data' # export - it cant be this path, only /home/airflow/rex/data
DATASET_FILE = 'healthcare-dataset-stroke-data.csv'
PARQUET_FILE = 'healthcare-dataset-stroke-data.parquet'
SECRET_JSON = 'beto-cloud-4a0aaf7a011b.json'

def build_gcp_client(secret_json):
    with open(secret_json) as f:
        gcp_settings = json.load(f)

    project_id = gcp_settings['project_id']
    gcp_credentials = service_account.Credentials.from_service_account_info(gcp_settings)

    bq_client = bigquery.Client(project=project_id, credentials=gcp_credentials)
    return bq_client

def load_csv_data(csv_path_file):
    df = pd.read_csv(csv_path_file)
    return df

def get_schema_from_parquet(parquet_path_file):
    pfile = pq.read_table(parquet_path_file)
    schema = []
    for item in pfile.schema:
        item_schema = {'name': str(item.name), 'type': str(item.type)}
        schema.append(item_schema)

    return schema

def get_data():
    kaggle.api.authenticate() ## authenticate in kaggle

    kaggle.api.dataset_download_files('fedesoriano/stroke-prediction-dataset', path=DATA_PATH, unzip=True) ## download the dataset in the DATA_PATH

# get_data() ## 1 dag

def transform_to_parquet():
    df = load_csv_data(DATA_PATH + '\\' + DATASET_FILE) ## loads the DATASET_FILE
    df.to_parquet(DATA_PATH + '\\' + PARQUET_FILE) ## save the PARQUET_FILE

# transform_to_parquet() ## 2 dag

def load_schema_to_bucket():
    schema = get_schema_from_parquet(DATA_PATH + '\\' + PARQUET_FILE)
    ## TODO send schema to GCP bucket

# load_schema_to_bucket() ## 3 dag

def load_parquet_to_bucket():
    return 0 
    ## TODO send parquet to bucket: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet?hl=pt-br

# load_parquet_to_bucket() ## 4 dag

def load_data_to_bq():
    bq_client = build_gcp_client(SECRET_JSON)
    ## send data

# load_data_to_bq() ## 5 dag

## pytest