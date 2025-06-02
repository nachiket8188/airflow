import sys
import datetime
import pandas as pd
import os
import boto3
import logging

from dotenv import load_dotenv
from etl.aws import (fetch_parameter, assume_role)
from airflow.decorators import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator

load_dotenv()

s3_location_parameter_name = "/retail_sales/bucket_prefix"
aws_role_parameter_name = "/airflow/airflow_user"

s3_bucket = fetch_parameter(s3_location_parameter_name)
logging.info(f"Current Directory : {os.getcwd()}")

aws_role_arn = os.getenv("AWS_ROLE_ARN")
logging.info(f"S3 Bucket : {s3_bucket}")
assumed_session = assume_role(role_arn=aws_role_arn)

if s3_bucket is None:
    sys.exit("Exiting. Something went wrong. Check the logs.")
else:
    s3_client = assumed_session.client("s3")
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix="retail_sales/")
    file_list = [file["Key"] for file in response["Contents"]]
    logging.info(f"file_list : {file_list}")
    filtered_files = [f for f in file_list if ".csv" in f]
    filtered_file_names = [i.split('/')[-1] for i in filtered_files]
    logging.info(filtered_file_names)

@dag(dag_id="S3_To_Snowflake", start_date=datetime.datetime(2025, 5, 25), schedule="@daily")
def generate_dag():
    @task(task_id="list_download_s3")
    def list_and_download_s3_files(ip_s3_bukcet:str):
        """Takes as input name of S3 bucket and downloads expected CSV file from it. Returns the pandas Dataframe
        for the downloaded file.
        """
        session = assumed_session
        s3_client = session.client(service_name="s3")
        s3_client.download_file(s3_bucket, filtered_files[0], f"./dags/mark_1/temp/{filtered_file_names[0]}")
        logging.info(f"Downloaded file : {filtered_files}")

    # EmptyOperator(task_id="Task_A")

    @task(task_id="process_CSV")
    def read_transform_csv():
        """ Reads the CSV into a dataframe and creates a new dataframe by summarizing data. """
        df = pd.read_csv(f"./dags/mark_1/temp/{filtered_file_names[0]}")
        unique_products = df['Product Category'].unique()
        logging.info(f"Unique Product Cats. : {unique_products}")
        select_cols_df = df[['Product Category', 'Total Amount']].copy()
        grouped_df = select_cols_df.group_by('Product Category').sum()
        logging.info(grouped_df)

    list_and_download_s3_files(s3_bucket)
    read_transform_csv()

generate_dag()