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
            database=conn_params.get('db'),
            schema=conn_params.get('schema')
        )
    except Exception as e:
        logger.info(f"Ran into an error while connecting to Snowflake: {e}")
    finally:
        return conn