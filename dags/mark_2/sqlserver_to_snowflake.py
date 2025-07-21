import datetime
import os
import logging

from dotenv import load_dotenv
from airflow.decorators import dag, task
from airflow.sdk import Param
from etl.snowflake import (create_connection)
from etl.mssql import (get_connection, get_schema_table_list)

load_dotenv()
logger = logging.getLogger(__name__)

SQLSERVER_DRIVER = os.getenv("SQLSERVER_DRIVER")
SQLSERVER_HOST = os.getenv("SQLSERVER_HOST")
SQLSERVER_PORT = os.getenv("SQLSERVER_PORT")
SQLSERVER_DB = os.getenv("SQLSERVER_DB")
SQLSERVER_USER = os.getenv("SQLSERVER_USER")
SQLSERVER_PASS = os.getenv("SQLSERVER_PASS")

SQL_CONNECTION_STRING=(f"Driver={SQLSERVER_DRIVER};Server={SQLSERVER_HOST},{SQLSERVER_PORT};Database={SQLSERVER_DB};Encrypt=yes;"
                       f"TrustServerCertificate=yes;UID={SQLSERVER_USER};PWD={SQLSERVER_PASS}")

conn = get_connection(SQL_CONNECTION_STRING)

schema_table_list = get_schema_table_list(conn)
modified_schema_table_list = list(str(pair[:2]) for pair in schema_table_list) # removes rowcount from tuple as it is
# not required here.

@dag(dag_id='sqlserver_to_snowflake', start_date=datetime.datetime(2025, 6, 27)
     , params={
        "table_name":Param(
            "N/A", type="string", enum = modified_schema_table_list, description="Select the correct pair of "
                                                                                 "Schema and Table name that should be "
                                                                                 "loaded into Snowflake. if the table is"
                                                                                 " already present, downstream tasks"
                                                                                 " will be skipped."
        )
    })
def generate_dag():
    @task(task_id='do_validation')
    def run_validation(**context) -> str:
        logger.info(context["params"]["table_name"])
        logger.info(type(context["params"]["table_name"]))
        selected_schema_name = tuple(context["params"]["table_name"])[0]
        selected_table_name = tuple(context["params"]["table_name"])[1]
        logger.info(f"The selected schema and table names are : {selected_schema_name}, {selected_table_name}")

    run_validation()

generate_dag()
