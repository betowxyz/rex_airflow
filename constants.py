import os
## constants
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = ROOT_DIR + "/" + "data"

DATASET_FILE = DATA_PATH + "/" + "healthcare-dataset-stroke-data.csv"

PARQUET_NAME = "healthcare-dataset-stroke-data.parquet"
PARQUET_FILE = DATA_PATH + "/" + PARQUET_NAME
PARQUET_BUCKET = "stroke-parquet"

SCHEMA_NAME = "schema.json"
SCHEMA_FILE = DATA_PATH + "/" + SCHEMA_NAME
SCHEMA_BUCKET = "stroke-schema"

BQ_DATASET = "rex_challenge"
BQ_TABLE = "stroke_data"
BQ_DESTINATION = BQ_DATASET + "." + BQ_TABLE

SECRET_JSON = ROOT_DIR + "/secret/" + "beto-cloud.json"