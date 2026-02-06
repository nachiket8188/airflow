import datetime
import os
import logging
import ast
import pandas as pd

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Param
from etl.snowflake import (create_connection)
from snowflake.connector.pandas_tools import write_pandas
from etl.mssql import (get_connection, get_schema_table_list)

logger = logging.getLogger(__name__)

config = {
    'user':'SNOWFLAKE_USER',
    'pass':'SNOWFLAKE_PASS',
    'account':'SNOWFLAKE_ACCT',
    'wh':'SNOWFLAKE_VWH',
    'db':'SNOWFLAKE_DB'
    # 'schema':'SNOWFLAKE_SCHEMA',
}

snowflake_conn_params = {
    k : os.getenv(v) for k,v in config.items()
}

SQLSERVER_DRIVER = os.getenv("SQLSERVER_DRIVER")
SQLSERVER_HOST = os.getenv("SQLSERVER_HOST")
SQLSERVER_PORT = os.getenv("SQLSERVER_PORT")
SQLSERVER_DB = os.getenv("SQLSERVER_DB")
SQLSERVER_USER = os.getenv("SQLSERVER_USER")
SQLSERVER_PASS = os.getenv("SQLSERVER_PASS")

SQL_CONNECTION_STRING=(f"Driver={SQLSERVER_DRIVER};Server={SQLSERVER_HOST},{SQLSERVER_PORT};Database={SQLSERVER_DB};Encrypt=no;"
                       f"TrustServerCertificate=yes;UID={SQLSERVER_USER};PWD={SQLSERVER_PASS}")

conn = get_connection(SQL_CONNECTION_STRING)

schema_table_list = get_schema_table_list(conn)

modified_schema_table_list = list(str(pair[:2]) for pair in schema_table_list) # removes rowcount from tuple as it is

@dag(dag_id='sqlserver_to_snowflake', start_date=datetime.datetime(2025, 6, 27)
     , params={
        "table_name":Param(
            "N/A", type="string", enum = modified_schema_table_list, description="Select the correct pair of "
                                                                                 "Schema and Table name that should be "
                                                                                 "loaded into Snowflake. if the table is"
                                                                                 " already present, downstream tasks"
                                                                                 " will be skipped."
        )
    }
     )
def generate_dag():
    @task(task_id='do_validation')
    def process_input(**context) -> tuple:
        selected_schema_and_table_combination = context["params"]["table_name"]
        selected_schema_name = ast.literal_eval(selected_schema_and_table_combination)[0]
        selected_table_name = ast.literal_eval(selected_schema_and_table_combination)[1]
        return (selected_schema_name, selected_table_name)

    # @task(task_id='validation')
    @task.branch(task_id='validation_branch')
    def validation(input_tup: tuple) -> str:
        query = f"""select COUNT(*) from AdventureWorks.{input_tup[0]}.{input_tup[1]};
        """
        snowflake_conn = create_connection(snowflake_conn_params)
        cursor = snowflake_conn.cursor()
        cursor.execute(query)
        table_row_count = cursor.fetchone()
        logger.info(table_row_count[0])
        if table_row_count[0] == 0:
            logger.info('Proceed')
            return "read_data"
        else:
            logger.info('Halt')
            return "skip_and_end_task"

    @task(task_id='read_data')
    def read_data(input_tup: tuple) -> pd.DataFrame:
        query = f"""select * from {input_tup[0]}.{input_tup[1]};
        """
        df = pd.read_sql(sql=query, con=conn)
        return {"dataframe": df, "tuple": input_tup}

    @task(task_id='write_data')
    def write_data(input_dict: dict):
        input_df = input_dict["dataframe"]
        input_df.columns = input_df.columns.str.upper()

        # Assuming your original column is already a proper pandas datetime object
        input_df['MODIFIEDDATE'] = input_df['MODIFIEDDATE'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')

        input_tup =input_dict["tuple"]
        snowflake_conn = create_connection(snowflake_conn_params)

        snowflake_conn.cursor().execute("ALTER SESSION SET TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9';")

        schema = input_tup[0].upper()
        table = input_tup[1].upper()
        full_table = f"{schema}.{table}"
        success, nchunks, nrows, _ = write_pandas(conn=snowflake_conn, df=input_df,
                                                  database=list({snowflake_conn_params['db']})[0],
                                                  schema=schema,
                                                  table_name=table)
        logger.info(f"success : {success} \n nchunks : {nchunks} \n nrows : {nrows} \n _ : {_}")
        conn.close()

    @task(task_id='skip_and_end_task')
    def end_dag_early():
        # Optional: You can raise AirflowSkipException here to make the log clearer
        raise AirflowSkipException("Destination Data row count was non-zero. Skipping downstream tasks.")
        logger.info("DAG finished execution via the 'Halt' path (existing data found).")
        pass

    table_tuple = process_input()
    decision_task = validation(table_tuple)

    read_data_task_output = read_data(table_tuple)
    write_operation = write_data(read_data_task_output)

    skip_operation = end_dag_early()

    decision_task >> [read_data_task_output, skip_operation]

generate_dag()
