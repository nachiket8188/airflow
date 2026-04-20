import datetime
import os
import logging
import ast
import pandas as pd

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Param
from etl.snowflake import (create_connection)
# from etl.snowflake import (create_connection, get_table_column_types)
from snowflake.connector.pandas_tools import write_pandas
from etl.mssql import (
    get_connection,
    # get_special_columns,
    # get_safe_select_query,
    get_schema_table_list,
    get_temporal_columns,
)

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
    """
    Learning note:
    For raw landing, keep temporal and special/spatial source fields in
    Snowflake as VARCHAR-like columns where needed, then do explicit typed
    conversion in a later Snowflake step. This DAG currently applies only the
    temporal normalization needed before the initial load.
    """

    """
    text_compatible_snowflake_types = {
        "TEXT",
        "VARCHAR",
        "STRING",
    }
    """

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
    def read_data(input_tup: tuple) -> dict:
        # query = get_safe_select_query(conn, input_tup[0], input_tup[1])
        # logger.info("Using metadata-driven extract query for %s.%s", input_tup[0], input_tup[1])
        query = f"""select * from {input_tup[0]}.{input_tup[1]};
        """
        df = pd.read_sql(sql=query, con=conn)
        temporal_columns = get_temporal_columns(conn, input_tup[0], input_tup[1])
        # special_columns = get_special_columns(conn, input_tup[0], input_tup[1])
        return {
            "dataframe": df,
            "tuple": input_tup,
            "temporal_columns": temporal_columns,
            # "special_columns": special_columns,
        }

    @task(task_id='write_data')
    def write_data(input_dict: dict):
        input_df = input_dict["dataframe"]
        input_df.columns = input_df.columns.str.upper()
        temporal_columns = {
            column_name.upper(): data_type
            for column_name, data_type in input_dict.get("temporal_columns", {}).items()
        }
        for column_name, data_type in temporal_columns.items():
            if column_name not in input_df.columns:
                continue
            original_non_null = input_df[column_name].notna().sum()
            input_df[column_name] = pd.to_datetime(
                input_df[column_name],
                errors="coerce",
            )
            coerced_to_null = original_non_null - input_df[column_name].notna().sum()
            if coerced_to_null:
                logger.warning(
                    "Column %s had %s invalid date values coerced to null before Snowflake load.",
                    column_name,
                    coerced_to_null,
                )
            logger.info(
                "Prepared temporal column %s from SQL Server type %s with pandas dtype %s.",
                column_name,
                data_type,
                input_df[column_name].dtype,
            )

        input_tup =input_dict["tuple"]
        snowflake_conn = create_connection(snowflake_conn_params)
        # Special/spatial-type compatibility checks are intentionally commented
        # out for now. The current intended raw-layer design is to keep such
        # columns as VARCHAR-like columns in Snowflake and convert later.
        #
        # target_column_types = get_table_column_types(
        #     snowflake_conn,
        #     snowflake_conn_params["db"],
        #     input_tup[0].upper(),
        #     input_tup[1].upper(),
        # )
        # special_columns = {
        #     column_name.upper(): data_type
        #     for column_name, data_type in input_dict.get("special_columns", {}).items()
        # }
        # incompatible_columns = []
        # for column_name, source_type in special_columns.items():
        #     target_type = target_column_types.get(column_name)
        #     if target_type and target_type not in text_compatible_snowflake_types:
        #         incompatible_columns.append((column_name, source_type, target_type))
        #
        # if incompatible_columns:
        #     formatted_columns = ", ".join(
        #         f"{column_name} ({source_type} -> {target_type})"
        #         for column_name, source_type, target_type in incompatible_columns
        #     )
        #     raise ValueError(
        #         "Destination Snowflake table is not compatible with special SQL Server types "
        #         f"serialized as text: {formatted_columns}. Load these columns into VARCHAR/TEXT "
        #         "in the raw table first, then cast in a downstream Snowflake transform."
        #     )

        schema = input_tup[0].upper()
        table = input_tup[1].upper()
        full_table = f"{schema}.{table}"
        success, nchunks, nrows, _ = write_pandas(conn=snowflake_conn, df=input_df,
                                                  database=list({snowflake_conn_params['db']})[0],
                                                  schema=schema,
                                                  table_name=table,
                                                  use_logical_type=True)
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
