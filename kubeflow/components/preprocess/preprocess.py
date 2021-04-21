from kfp.components import InputPath, OutputPath, create_component_from_func

def preprocess():
    '''
        Get GCP raw data, pre process and load to bucket
    '''

    import pandas as pd
    from sklearn import preprocessing

    from google.cloud import bigquery, storage

    def build_storage_client(project_id):
        """
        Build Storage client to perform requests to GCP buckets
        Params:
            project_id: the respective project of GCP
        """

        # step.apply(gcp.use_gcp_secret('user-gcp-sa')) in the dsl.ContainerOP()
        storage_client = storage.Client(project_id)
        return storage_client

    def load_parquet_to_bucket(storage_client, bucket_name, file_name):
        """
            Load file to GCP bucket
        """

        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        with open(file_name, "rb") as f:
            blob.upload_from_file(f)


    def normalize_dataframe(df):
        '''
            Receive a DataFrame and normalize using min max scaler (scikit)
        '''

        x = df.values
        column_names = list(df.columns)
        min_max_scaler = preprocessing.MinMaxScaler()
        x_scaled = min_max_scaler.fit_transform(x)
        df_normalized = pd.DataFrame(x_scaled, column_names)

        return df_normalized

    def get_bucket_data(storage_client, bucket_name, file_name):
        '''
            Get file from bucket and save locally
        '''

        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.download_to_filename(file_name)

    def parquet_to_dataframe(file_name):
        '''
            Read parquet file as DataFrame object
        '''

        df = pd.read_parquet(file_name, engine='pyarrow')
        return df

    def drop_null(df):
        '''
            Drop Null rows of given DataFrame
        '''

        df = df.dropna()
        return df

    def one_hot_encoding(df, columns_to_one_hot_encode):
        '''
            One hot encoding DataFrame
        '''

        for column in columns_to_one_hot_encode:
            one_hot = pd.get_dummies(df[column])
            df = df.drop(column, axis = 1)
            df = df.join(one_hot)

        return df

    def _preprocess_data():

        # Get raw data
        storage_client = build_storage_client('beto-cloud')
        raw_data_bucket_name = 'stroke-parquet'
        raw_data_file_name = 'stroke.parquet'
        get_bucket_data(storage_client, raw_data_bucket_name, raw_data_file_name)

        ## columns  to one hot encode
        columns_to_one_hot_encode = [
                'gender', 'ever_married', 'work_type', 'Residence_type', 'smoking_status' 
            ]

        # Pre Processment
        df = parquet_to_dataframe(raw_data_file_name)
        df = drop_null(df)
        df = one_hot_encoding(df)
        df = normalize_dataframe(df)

        # Load train data to train bucket
        train_data_bucket_name = 'stroke-train-parquet'
        train_data_file_name = 'stroke_train.parquet'
        load_parquet_to_bucket(storage_client, train_data_bucket_name, train_data_file_name)

if __name__ == '__main__':
    preprocess_op = create_component_from_func(
        preprocess,
        output_component_file='preprocess.yaml',
        base_image='python:3.8',
        packages_to_install=[
            'pandas==1.0.3', 
            'google-cloud-bigquery==2.13.1,',
            'google-cloud-bigquery-storage==2.3.0,',
            'google-cloud-bigtable==1.7.0,',
            'google-cloud-container==2.3.1,',
            'google-cloud-core==1.6.0,',
            'google-cloud-dlp==3.0.1,',
            'google-cloud-language==2.0.0,',
            'google-cloud-secret-manager==2.3.0,',
            'google-cloud-spanner==3.2.0,',
            'google-cloud-speech==2.2.0,',
            'google-cloud-storage==1.36.2,',
            'google-cloud-texttospeech==2.2.0,',
            'google-cloud-translate==3.0.2,',
            'google-cloud-videointelligence==2.0.0,',
            'google-cloud-vision==2.2.0,',
            ]
    )