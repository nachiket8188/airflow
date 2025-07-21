import os
import logging

import pyodbc
from dotenv import load_dotenv
from pyodbc import connect
import pandas as pd

logger = logging.getLogger(__name__)

def get_connection(conn_string:str) -> pyodbc.Connection:
    """
    Creates an ODBC connection to SQL Server Instance based on input parameters and returns the Connection obj.
    """
    conn = None
    try:
        conn = connect(conn_string)
    except Exception as e:
        print(f"Ran into an error while connecting to SQL-Server: {e}")
        # logger.info(f"Ran into an error while connecting to SQL-Server: {e}")
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