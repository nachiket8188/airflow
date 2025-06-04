import datetime
import pandas as pd
import os
import logging

from dotenv import load_dotenv
from etl.aws import (fetch_parameter, assume_role)
from etl.snowflake import (create_connection)
from airflow.decorators import dag, task
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()
logger = logging.getLogger(__name__)

s3_location_parameter_name = "/retail_sales/bucket_prefix"
aws_role_parameter_name = "/airflow/airflow_user"

s3_bucket = fetch_parameter(s3_location_parameter_name)

aws_role_arn = os.getenv("AWS_ROLE_ARN")
assumed_session = assume_role(role_arn=aws_role_arn)

# wrapping credentials into a dict to be passed for Snowflake Connection Creation
config = {
    'user':'SNOWFLAKE_USER',
    'pass':'SNOWFLAKE_PASS',
    'account':'SNOWFLAKE_ACCT',
    'wh':'SNOWFLAKE_VWH',
    'db':'SNOWFLAKE_DB',
    'schema':'SNOWFLAKE_SCHEMA',
}

snowflake_conn_params = {
    k : os.getenv(v) for k,v in config.items()
}

@dag(dag_id="S3_To_Snowflake", start_date=datetime.datetime(2025, 5, 25), schedule="@daily")
def generate_dag():
    @task(task_id="list_download_s3")
    def list_and_download_s3_files(ip_s3_bukcet:str) -> list:
        """
        Takes as input name of S3 bucket and downloads expected CSV file from it. Returns files in a list.
        """
        s3_client = assumed_session.client("s3")
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix="retail_sales/")
        file_list = [file["Key"] for file in response["Contents"]]
        logger.info(f"file_list : {file_list}")
        filtered_files = [f for f in file_list if ".csv" in f]
        filtered_file_names = [i.split('/')[-1] for i in filtered_files]
        logger.info(filtered_file_names)

        # download files to local machine
        s3_client.download_file(s3_bucket, filtered_files[0], f"/Users/nachiket/Projects/airflow_dev/dags/mark_1/"
                                                              f"temp/{filtered_file_names[0]}")
        logger.info(f"Downloaded file : {filtered_files}")
        return filtered_file_names


    @task(task_id="transform_CSV")
    def read_transform_csv(file_name_list:list) -> pd.api.typing.DataFrameGroupBy:
        """ Reads the CSV into a dataframe and creates a new dataframe by summarizing data. """
        df = pd.read_csv(f"/Users/nachiket/Projects/airflow_dev/dags/mark_1/temp/{file_name_list[0]}")
        # unique_products = df['Product Category'].unique()
        select_cols_df = df[['Product Category', 'Total Amount']].copy()
        select_cols_df.rename(columns={'Product Category':'PRODUCT_CATEGORY'}, inplace=True)
        processed_df = (select_cols_df.groupby('PRODUCT_CATEGORY', as_index=False)
                        .agg(TOTAL_AMOUNT=("Total Amount", "sum")))
                        # as_index=False makes sure that the index for rows are not lost after performing GroupBy
        return processed_df


    @task(task_id="load_into_snowflake")
    def load_into_snowflake(ip_df:pd.api.typing.DataFrameGroupBy):
        """
        Accepts a Grouped DataFrame as input to be inserted into a Snowflake table. If already existing,
        DB table is truncated.
        """
        conn = create_connection(snowflake_conn_params)
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE SALES_BY_CATEGORY;")
        success, nchunks, nrows, _ = write_pandas(conn=conn, df=ip_df, table_name="SALES_BY_CATEGORY")
        logger.info(f"success : {success} \n nchunks : {nchunks} \n nrows : {nrows} \n _ : {_}")
        conn.close()

    load_into_snowflake(read_transform_csv(list_and_download_s3_files(s3_bucket)))

generate_dag()