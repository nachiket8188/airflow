import snowflake.connector
import logging

logger = logging.getLogger(__name__)


def create_connection(conn_params:dict) -> snowflake.connector.SnowflakeConnection:
    """ Creates a Snowflake Connection with input parameters and returns the Connection object. """
    conn = None
    try:
        conn = snowflake.connector.connect(
            user=conn_params.get('user'),
            password=conn_params.get('pass'),
            account=conn_params.get('account'),
            warehouse=conn_params.get('wh'),
            database=conn_params.get('db')
            # schema=conn_params.get('schema')
        )
    except Exception as e:
        logger.info(f"Ran into an error while connecting to Snowflake: {e}")
    finally:
        return conn


# def get_table_column_types(
#     conn: snowflake.connector.SnowflakeConnection,
#     database: str,
#     schema: str,
#     table: str,
# ) -> dict[str, str]:
#     """
#     Returns Snowflake destination column types keyed by upper-cased column name.
#     """
#     cursor = conn.cursor()
#     cursor.execute(
#         f"""
#         SELECT COLUMN_NAME, DATA_TYPE
#         FROM {database}.INFORMATION_SCHEMA.COLUMNS
#         WHERE TABLE_SCHEMA = %s
#           AND TABLE_NAME = %s
#         ORDER BY ORDINAL_POSITION
#         """,
#         (schema, table),
#     )
#     return {column_name.upper(): data_type.upper() for column_name, data_type in cursor.fetchall()}
