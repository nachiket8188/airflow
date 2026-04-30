import datetime
import logging
import os

import pandas as pd
from airflow.decorators import dag, task
from snowflake.connector.pandas_tools import write_pandas

from etl.mssql import (
    get_connection,
    get_safe_select_query,
    get_schema_table_list,
    get_temporal_columns,
)
from etl.snowflake import create_connection

logger = logging.getLogger(__name__)

config = {
    "user": "SNOWFLAKE_USER",
    "pass": "SNOWFLAKE_PASS",
    "account": "SNOWFLAKE_ACCT",
    "wh": "SNOWFLAKE_VWH",
    "db": "SNOWFLAKE_DB",
}

snowflake_conn_params = {
    k: os.getenv(v) for k, v in config.items()
}

SQLSERVER_DRIVER = os.getenv("SQLSERVER_DRIVER")
SQLSERVER_HOST = os.getenv("SQLSERVER_HOST")
SQLSERVER_PORT = os.getenv("SQLSERVER_PORT")
SQLSERVER_DB = os.getenv("SQLSERVER_DB")
SQLSERVER_USER = os.getenv("SQLSERVER_USER")
SQLSERVER_PASS = os.getenv("SQLSERVER_PASS")

SQL_CONNECTION_STRING = (
    f"Driver={SQLSERVER_DRIVER};Server={SQLSERVER_HOST},{SQLSERVER_PORT};Database={SQLSERVER_DB};Encrypt=no;"
    f"TrustServerCertificate=yes;UID={SQLSERVER_USER};PWD={SQLSERVER_PASS}"
)


def _destination_row_count(snowflake_conn, schema: str, table: str) -> int:
    query = f"""
        select COUNT(*)
        from {snowflake_conn_params["db"]}.{schema}.{table}
    """
    cursor = snowflake_conn.cursor()
    try:
        cursor.execute(query)
        return cursor.fetchone()[0]
    finally:
        cursor.close()


def _normalize_temporal_columns(
    input_df: pd.DataFrame,
    temporal_columns: dict[str, str],
    qualified_table_name: str,
) -> pd.DataFrame:
    normalized_df = input_df.copy()
    normalized_df.columns = normalized_df.columns.str.upper()

    upper_temporal_columns = {
        column_name.upper(): data_type
        for column_name, data_type in temporal_columns.items()
    }
    for column_name, data_type in upper_temporal_columns.items():
        if column_name not in normalized_df.columns:
            continue

        original_non_null = normalized_df[column_name].notna().sum()
        normalized_df[column_name] = pd.to_datetime(
            normalized_df[column_name],
            errors="coerce",
        )
        coerced_to_null = original_non_null - normalized_df[column_name].notna().sum()
        if coerced_to_null:
            logger.warning(
                "%s column %s had %s invalid date values coerced to null before Snowflake load.",
                qualified_table_name,
                column_name,
                coerced_to_null,
            )
        logger.info(
            "%s prepared temporal column %s from SQL Server type %s with pandas dtype %s.",
            qualified_table_name,
            column_name,
            data_type,
            normalized_df[column_name].dtype,
        )

    return normalized_df


@dag(
    dag_id="sqlserver_to_snowflake_all_tables",
    start_date=datetime.datetime(2025, 6, 27),
    schedule=None,
    catchup=False,
)
def generate_dag():
    """
    Loads all approved AdventureWorks SQL Server tables into matching Snowflake
    tables. The approved schema/table combinations come from get_schema_table_list.
    """

    @task(task_id="load_all_empty_destination_tables")
    def load_all_empty_destination_tables() -> None:
        sqlserver_conn = None
        snowflake_conn = None

        try:
            sqlserver_conn = get_connection(SQL_CONNECTION_STRING)
            snowflake_conn = create_connection(
                {
                    **snowflake_conn_params,
                    "autocommit": False,
                }
            )
            if sqlserver_conn is None:
                raise RuntimeError("Could not create SQL Server connection.")
            if snowflake_conn is None:
                raise RuntimeError("Could not create Snowflake connection.")

            schema_table_list = get_schema_table_list(sqlserver_conn)
            total_tables = len(schema_table_list)
            logger.info(
                "Mark 4 discovered %s schema/table combinations from get_schema_table_list.",
                total_tables,
            )

            for table_number, table_info in enumerate(schema_table_list, start=1):
                source_schema, source_table, source_row_count = table_info[:3]
                source_qualified_table = f"{source_schema}.{source_table}"
                target_schema = source_schema.upper()
                target_table = source_table.upper()
                target_qualified_table = f"{target_schema}.{target_table}"

                logger.info(
                    "Table %s out of %s: processing %s.",
                    table_number,
                    total_tables,
                    source_qualified_table,
                )
                logger.info(
                    "Source row count for %s: %s.",
                    source_qualified_table,
                    source_row_count,
                )

                destination_row_count = _destination_row_count(
                    snowflake_conn,
                    target_schema,
                    target_table,
                )
                logger.info(
                    "Destination row count for %s: %s.",
                    target_qualified_table,
                    destination_row_count,
                )

                if destination_row_count != 0:
                    logger.info(
                        "Skipping %s because the destination Snowflake table already contains %s rows.",
                        source_qualified_table,
                        destination_row_count,
                    )
                    continue

                if source_row_count == 0:
                    logger.info(
                        "No rows to load for %s. Destination table is already empty.",
                        source_qualified_table,
                    )
                    continue

                query = get_safe_select_query(
                    sqlserver_conn,
                    source_schema,
                    source_table,
                )
                input_df = pd.read_sql(sql=query, con=sqlserver_conn)
                temporal_columns = get_temporal_columns(
                    sqlserver_conn,
                    source_schema,
                    source_table,
                )
                input_df = _normalize_temporal_columns(
                    input_df,
                    temporal_columns,
                    source_qualified_table,
                )

                try:
                    success, nchunks, nrows, output = write_pandas(
                        conn=snowflake_conn,
                        df=input_df,
                        database=snowflake_conn_params["db"],
                        schema=target_schema,
                        table_name=target_table,
                        use_logical_type=True,
                    )

                    if not success:
                        raise RuntimeError(
                            f"write_pandas reported failure for {source_qualified_table}. "
                            f"chunks={nchunks}, rows={nrows}, output={output}"
                        )

                    snowflake_conn.commit()
                    logger.info(
                        "Successfully loaded %s into Snowflake table %s. chunks=%s, rows_loaded=%s.",
                        source_qualified_table,
                        target_qualified_table,
                        nchunks,
                        nrows,
                    )
                except Exception:
                    logger.exception(
                        "Load failed for %s. Rolling back only the in-flight work for this table.",
                        source_qualified_table,
                    )
                    snowflake_conn.rollback()
                    raise

            logger.info(
                "Mark 4 completed. Processed %s schema/table combinations.",
                total_tables,
            )
        except Exception:
            if snowflake_conn is not None:
                logger.info(
                    "Mark 4 stopped after a table-level failure. Previously committed tables remain loaded."
                )
            raise
        finally:
            if sqlserver_conn is not None:
                sqlserver_conn.close()
            if snowflake_conn is not None:
                snowflake_conn.close()

    load_all_empty_destination_tables()


generate_dag()
