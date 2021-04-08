import pandas as pd

from pyarrow.parquet import ParquetFile

import pyarrow.parquet as pq

def load_csv_data(csv_path_file):
    df = pd.read_csv(csv_path_file)
    return df

def transform_to_parquet(df, parquet_path_file):
    parquet = df.to_parquet(parquet_path_file)
    return parquet

def get_schema_from_parquet_2(parquet_path_file):
    pfile = pq.read_table(parquet_path_file)
    for each in pfile.schema:
        print(each) # TODO create table_schema dictionary and return
    return 0
 
def main():
    data_path = "C:\\Users\\Stefano\\Desktop\\b\\rex_challenge\\data\\"

    csv_path_file = data_path + "imdb.csv"
    df = load_csv_data(csv_path_file)

    parquet_path_file = data_path + "imdb.parquet"
    parquet = transform_to_parquet(df, parquet_path_file)

    schema = get_schema_from_parquet(parquet_path_file)
    schema = get_schema_from_parquet_2(parquet_path_file)

    # TODO 1 move all code to DAGs
    # TODO 2 send parquet schema to bucket
    # TODO 3 send parquet data to bucket (another)
    # TODO 4 get parquet from bucket data ? (or we can get from csv?) and send to BQ

if __name__ == "__main__":
    main()