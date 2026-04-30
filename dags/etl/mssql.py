import os
import logging

import pyodbc
from dotenv import load_dotenv
from pyodbc import connect
import pandas as pd

logger = logging.getLogger(__name__)

"""
    
"""
DATE_DATA_TYPES = {
    "date",
    "datetime",
    "datetime2",
    "datetimeoffset",
    "smalldatetime",
}

DATE_COLUMN_NAME_SUFFIXES = (
    "date",
    "datetime",
    "timestamp",
)

SPECIAL_TYPE_SELECTORS = {
    "geography": "[{column_name}].STAsText() AS [{column_name}]",
    "geometry": "[{column_name}].STAsText() AS [{column_name}]",
    "hierarchyid": "[{column_name}].ToString() AS [{column_name}]",
    "xml": "CAST([{column_name}] AS NVARCHAR(MAX)) AS [{column_name}]",
    "sql_variant": "CAST([{column_name}] AS NVARCHAR(MAX)) AS [{column_name}]",
    "uniqueidentifier": "CAST([{column_name}] AS VARCHAR(36)) AS [{column_name}]",
    "binary": "CONVERT(VARCHAR(MAX), [{column_name}], 2) AS [{column_name}]",
    "varbinary": "CONVERT(VARCHAR(MAX), [{column_name}], 2) AS [{column_name}]",
    "image": "CONVERT(VARCHAR(MAX), [{column_name}], 2) AS [{column_name}]",
    "rowversion": "CONVERT(VARCHAR(MAX), [{column_name}], 2) AS [{column_name}]",
    "timestamp": "CONVERT(VARCHAR(MAX), [{column_name}], 2) AS [{column_name}]",
}

SPECIAL_SQLSERVER_TYPES = set(SPECIAL_TYPE_SELECTORS)

def get_connection(conn_string:str) -> pyodbc.Connection:
    """
    Creates an ODBC connection to SQL Server Instance based on input parameters and returns the Connection obj.
    """
    conn = None
    try:
        conn = connect(conn_string)
    except Exception as e:
        print(f"Ran into an error while connecting to SQL-Server: {e}")
    finally:
        return conn

def get_schema_table_list(conn:pyodbc.Connection) -> list:
    """
    This function takes an i/p a Connection to a SQL-Server instance and returns a list of tuples where each tuple
    contains schema name, table name and record count of a table for each table within pre-determined DB.
    """
    query = """
    select
	distinct t1.TABLE_SCHEMA,
	t1.TABLE_NAME,
	SUM(p.[rows]) as ROW_COUNT
    from
        INFORMATION_SCHEMA.TABLES t1
    inner join sys.tables t2 on
        t1.TABLE_NAME = t2.name
    inner join sys.partitions p on
        t2.object_id = p.object_id
    where
        t1.TABLE_TYPE = 'BASE TABLE'
        and t1.TABLE_SCHEMA NOT IN ('dbo')
        and p.index_id IN (0, 1)
    group by
        t1.TABLE_SCHEMA,
        t1.TABLE_NAME
    order by
	1,
	2;
    """
    logger.info("Querying names of available schemas and tables within.")
    # Initially, the goal was to read in records as a Pandas DF and then convert to dict for further processing
    # However, for simplicity, later i decided to use tuples for selection options in Form. Hence, now, reading rows as tuples
    # schema_tables_df = pd.read_sql(query, conn)
    # schema_tables_dict = (schema_tables_df.groupby('TABLE_SCHEMA')
    #                       .apply(lambda x:list(zip(x['TABLE_NAME'], x['ROW_COUNT']))).to_dict())
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    tables_list = [tuple(row) for row in rows]
    return tables_list

def get_safe_select_query(conn: pyodbc.Connection, schema: str, table: str) -> str:
    """
    Builds a SELECT statement for the given table that serializes SQL Server
    types that do not round-trip cleanly through pyodbc/pandas.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COLUMN_NAME, DATA_TYPE
        FROM   INFORMATION_SCHEMA.COLUMNS
        WHERE  TABLE_SCHEMA = ?
          AND  TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """,
        schema,
        table,
    )
    cols = cursor.fetchall()
    select_parts = []

    for col_name, data_type in cols:
        normalized_type = data_type.lower()
        selector = SPECIAL_TYPE_SELECTORS.get(normalized_type)
        if selector:
            logger.info(
                "Serializing special SQL Server column %s.%s.%s of type %s during extract.",
                schema,
                table,
                col_name,
                normalized_type,
            )
            select_parts.append(selector.format(column_name=col_name))
            continue

        select_parts.append(f"[{col_name}]")

    return f"SELECT {', '.join(select_parts)} FROM [{schema}].[{table}]"


def get_temporal_columns(conn: pyodbc.Connection, schema: str, table: str) -> dict[str, str]:
    """
    Returns source column names that should be normalized as dates/timestamps
    before loading to Snowflake, along with their SQL Server data types.

    Primary signal: SQL Server date-like data types.
    Fallback signal: column names that clearly look date-like, which handles
    tables where dates are stored in string/varchar columns.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COLUMN_NAME, DATA_TYPE
        FROM   INFORMATION_SCHEMA.COLUMNS
        WHERE  TABLE_SCHEMA = ?
          AND  TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """,
        schema,
        table,
    )
    temporal_columns = {}
    for column_name, data_type in cursor.fetchall():
        normalized_name = column_name.replace("_", "").lower()
        normalized_type = data_type.lower()
        if (
            normalized_type in DATE_DATA_TYPES
            or normalized_name.endswith(DATE_COLUMN_NAME_SUFFIXES)
        ):
            temporal_columns[column_name] = normalized_type

    return temporal_columns


# def get_special_columns(conn: pyodbc.Connection, schema: str, table: str) -> dict[str, str]:
#     """
#     Returns SQL Server columns whose source data types need special handling
#     during extraction/loading.
#     """
#     cursor = conn.cursor()
#     cursor.execute(
#         """
#         SELECT COLUMN_NAME, DATA_TYPE
#         FROM   INFORMATION_SCHEMA.COLUMNS
#         WHERE  TABLE_SCHEMA = ?
#           AND  TABLE_NAME = ?
#         ORDER BY ORDINAL_POSITION
#         """,
#         schema,
#         table,
#     )
#     special_columns = {}
#     for column_name, data_type in cursor.fetchall():
#         normalized_type = data_type.lower()
#         if normalized_type in SPECIAL_SQLSERVER_TYPES:
#             special_columns[column_name] = normalized_type
#
#     return special_columns
