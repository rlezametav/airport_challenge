import requests
import json
import pandas as pd
import boto3
import awswrangler as wr

from dagster import asset

@asset
def extract_data():
    response = requests.get(
        "https://think.cs.vt.edu/corgis/datasets/json/airlines/airlines.json"
    )
    json_raw_data = json.loads(response.text)
    return json_raw_data

@asset
def transform_data(extract_data):
    raw_df = pd.json_normalize(extract_data)
    raw_column_names = list(raw_df.columns)
    standarized_column_names = [
        x.lower().replace('.', '_').replace(' ', '_').replace('#_of_', 'num_')
        for x in raw_column_names
    ]
    raw_df.columns = standarized_column_names
    return raw_df

@asset(required_resource_keys={"aws_access_key_id", "aws_secret_access_key"})
def load_data_to_s3(context, transform_data) -> None:
    aws_session = boto3.Session(
        aws_access_key_id=context.resources.aws_access_key_id,
        aws_secret_access_key=context.resources.aws_secret_access_key,
        region_name='us-east-1'
    )

    wr.s3.to_csv(
        boto3_session=aws_session,
        df=transform_data,
        sep='|',
        index=False,
        header = True,
        path='s3://raw-dtlk-ralv/test',
        dataset=True,
        partition_cols=['time_year'],
        mode='overwrite_partitions',
        database='default',
        table='airlines'
    )