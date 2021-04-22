from datetime import timedelta

import kaggle

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from utils import *

from constants import *

def get_data():
    """
    DAG 1: Request stroke-prediction-dataset from kaggle (public dataset)
    """

    kaggle.api.authenticate()  ## authenticate in kaggle
    kaggle.api.dataset_download_files(
        "fedesoriano/stroke-prediction-dataset", path=DATA_PATH, unzip=True
    )  ## download the dataset in the DATA_PATH


def transform_to_parquet():
    """
    Dag 2: Transform the data requested to parquet
    """

    df = load_csv_data(DATASET_FILE)  ## loads the DATASET_FILE
    df.to_parquet(PARQUET_FILE)  ## save the PARQUET_FILE


def load_schema_to_bucket():
    """
    Dag 3: Load schema to bucket
    """

    schema = get_schema_from_parquet(PARQUET_FILE)  ## getting schema
    save_dict_as_json(schema, SCHEMA_FILE)  ## saving schema to json

    gcp_credentials, project_id = build_gcp_credentials(
        SECRET_JSON
    )  ## creating credentials object
    storage_client = build_storage_client(
        gcp_credentials, project_id
    )  ## building storage client

    load_to_bucket(storage_client, SCHEMA_BUCKET, SCHEMA_FILE, SCHEMA_NAME)


def load_parquet_to_bucket():
    """
    Dag 4: Load parquet to bucket
    """

    gcp_credentials, project_id = build_gcp_credentials(
        SECRET_JSON
    )  ## creating credentials object
    storage_client = build_storage_client(
        gcp_credentials, project_id
    )  ## building storage client

    load_to_bucket(storage_client, PARQUET_BUCKET, PARQUET_FILE, PARQUET_NAME)


def load_data_to_bq():
    """
    Dag 5: Load data to BigQuery
    """

    gcp_credentials, project_id = build_gcp_credentials(SECRET_JSON)
    bq_client = build_bq_client(gcp_credentials, project_id)

    df = load_csv_data(DATASET_FILE)  ## we can load from parquet too
    schema = read_json_to_dict(SCHEMA_FILE)
    bq_schema = generate_bq_schema(schema)
    table_id = project_id + "." + BQ_DESTINATION

    if not bq_table_exists(bq_client, table_id):  ## create table if don't exists
        create_bq_table(bq_client, bq_schema, table_id)

    numeric_columns = extract_numeric_columns(schema)  ## prepare df to BQ
    df_columns_to_numeric(df, numeric_columns)

    df.to_gbq(  ## send data to BQ
        destination_table=BQ_DESTINATION,
        project_id=project_id,
        credentials=gcp_credentials,
        if_exists="append",
    )


## Airflow DAG

default_args = {
    "owner": "lakshay",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="get_data",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    dagrun_timeout=timedelta(minutes=60),
)

t1 = PythonOperator(
    task_id="get_data",
    python_callable=get_data,
    op_kwargs=None,
    dag=dag,
)

t2 = PythonOperator(
    task_id="transform_to_parquet",
    python_callable=transform_to_parquet,
    op_kwargs=None,
    dag=dag,
)

t3 = PythonOperator(
    task_id="load_schema_to_bucket",
    python_callable=load_schema_to_bucket,
    op_kwargs=None,
    dag=dag,
)

t4 = PythonOperator(
    task_id="load_parquet_to_bucket",
    python_callable=load_parquet_to_bucket,
    op_kwargs=None,
    dag=dag,
)

t5 = PythonOperator(
    task_id="load_data_to_bq",
    python_callable=load_data_to_bq,
    op_kwargs=None,
    dag=dag,
)

t1 >> t2 >> t3 >> t4 >> t5

## Hint: starting airflow
## $ airflow initdb
## $ airflow webserver --port 8080
## $ airflow scheduler