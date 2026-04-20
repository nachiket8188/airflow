"""
Mark 3: Faker → Snowflake

Simulates daily OLTP activity on an AdventureWorks-style retail database.
At each run, Faker generates realistic records for Person, Address, Customer,
Product, SalesOrderHeader, and SalesOrderDetail, then loads them into Snowflake
via write_pandas — the same mechanism used in Mark 2.

Key design decisions:
  - Referential integrity is maintained by querying Snowflake for current max PKs
    and valid FK values before any generation begins (Option A).
  - Row counts for each entity are controlled via Airflow Params, visible in the
    Trigger DAG UI. Defaults reflect a realistic daily transaction volume.
  - SalesOrderDetail line counts are randomised per order (1–8, weighted toward
    2–4), so the total detail rows vary naturally across runs.
  - schedule=None — this DAG is intended for manual triggering during dbt
    development and testing, not for automated scheduling.
  - All generated DataFrames are converted to dicts for XCom serialisation
    (JSON-safe), then reconstructed as DataFrames inside each load task.

Note on XCom size: Airflow's default DB-backed XCom has a ~2 MB soft limit.
For the default row counts this is well within bounds. If you significantly
increase n_orders, consider switching to an XCom backend that supports larger
payloads (e.g. S3-backed XCom with the airflow-provider-amazon package).
"""

import datetime
import os
import logging

import pandas as pd
from airflow.decorators import dag, task
from airflow.sdk import Param
from snowflake.connector.pandas_tools import write_pandas

from etl.snowflake import create_connection
from mark_3.generators.reference_data import fetch_all_reference_data
from mark_3.generators.person import generate_persons, generate_addresses
from mark_3.generators.customer import generate_customers
from mark_3.generators.product import generate_products
from mark_3.generators.sales_order import generate_sales_orders

logger = logging.getLogger(__name__)

# ── Snowflake connection params ───────────────────────────────────────────────
# Follows the same pattern as Mark 1 and Mark 2.
config = {
    'user':    'SNOWFLAKE_USER',
    'pass':    'SNOWFLAKE_PASS',
    'account': 'SNOWFLAKE_ACCT',
    'wh':      'SNOWFLAKE_VWH',
    'db':      'SNOWFLAKE_DB',
}

snowflake_conn_params = {k: os.getenv(v) for k, v in config.items()}

# ── Default row counts ────────────────────────────────────────────────────────
# These values are pre-populated in the Trigger DAG form and can be overridden
# at run time. Set a param to 0 to skip generation for that entity entirely.
_DEFAULTS = {
    'n_persons':   15,   # New Person + Address records per run
    'n_customers': 10,   # New Customer records per run
    'n_products':   2,   # New Product records per run
    'n_orders':    25,   # New SalesOrderHeader records per run
}


# ── Shared load helper ────────────────────────────────────────────────────────
def _load_table(records: list, schema: str, table: str, sample_cols: list = None) -> None:
    """
    Reconstructs a DataFrame from a list of dicts (XCom payload), logs a sample,
    and loads the data into the specified Snowflake table via write_pandas.

    Args:
        records: List of dicts, as produced by DataFrame.to_dict('records').
        schema: Snowflake schema name (e.g. 'PERSON', 'SALES').
        table: Snowflake table name (e.g. 'PERSON', 'SALESORDERHEADER').
        sample_cols: Optional subset of columns to include in the logged sample.
    """
    if not records:
        logger.info(f"No records to load for {schema}.{table}. Skipping.")
        return

    df = pd.DataFrame(records)
    df.columns = df.columns.str.upper()

    logger.info(f"Loading {len(df)} records into {schema}.{table}.")

    displayable = df[sample_cols] if sample_cols and all(c in df.columns for c in sample_cols) else df
    logger.info(f"Sample (up to 5 rows):\n{displayable.head(5).to_string()}")

    conn = create_connection(snowflake_conn_params)
    conn.cursor().execute("ALTER SESSION SET TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9';")

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        database=snowflake_conn_params['db'],
        schema=schema,
        table_name=table,
    )
    logger.info(f"write_pandas → success: {success} | chunks: {nchunks} | rows written: {nrows}")
    conn.close()


# ── DAG definition ────────────────────────────────────────────────────────────
@dag(
    dag_id='faker_to_snowflake',
    start_date=datetime.datetime(2025, 6, 27),
    schedule=None,   # manual trigger only
    params={
        'n_persons': Param(
            _DEFAULTS['n_persons'],
            type='integer',
            minimum=0,
            description=(
                'Number of new Person records to generate. '
                'An equal number of Address records will also be created (1 address per person).'
            ),
        ),
        'n_customers': Param(
            _DEFAULTS['n_customers'],
            type='integer',
            minimum=0,
            description=(
                'Number of new Customer records to generate. '
                'Each customer is linked to a Person not yet registered as a customer. '
                'Capped automatically if insufficient eligible persons exist.'
            ),
        ),
        'n_products': Param(
            _DEFAULTS['n_products'],
            type='integer',
            minimum=0,
            description='Number of new Product records to generate.',
        ),
        'n_orders': Param(
            _DEFAULTS['n_orders'],
            type='integer',
            minimum=0,
            description=(
                'Number of new SalesOrderHeader records to generate. '
                'SalesOrderDetail lines are auto-generated at 1–8 per order '
                '(weighted toward 2–4), so total detail rows will vary each run.'
            ),
        ),
    },
)
def generate_dag():
    @task(task_id='fetch_reference_data')
    def fetch_reference_data() -> dict:
        """
        Queries Snowflake for current max PKs and valid FK values across all
        relevant tables. This data is used by the generators to assign correct
        IDs and maintain referential integrity without touching the source DB.
        """
        conn = create_connection(snowflake_conn_params)
        ref_data = fetch_all_reference_data(conn)
        conn.close()
        return ref_data

    @task(task_id='generate_all_data')
    def generate_all_data(ref_data: dict, **context) -> dict:
        """
        Generates all fake records for this run using the Faker library.
        Row counts are read from DAG params so they can be overridden at trigger time.

        Returns a single dict of {entity: list_of_records} for downstream load tasks.
        All DataFrames are serialised to lists of dicts for XCom compatibility.
        """
        params = context['params']
        n_persons   = params['n_persons']
        n_customers = params['n_customers']
        n_products  = params['n_products']
        n_orders    = params['n_orders']

        # ── Persons ───────────────────────────────────────────────────────────
        persons_df = generate_persons(n_persons, ref_data['max_business_entity_id'])
        new_person_ids = persons_df['BUSINESSENTITYID'].tolist() if not persons_df.empty else []

        # ── Addresses (1:1 with new persons) ──────────────────────────────────
        addresses_df = generate_addresses(
            n_persons,
            ref_data['max_address_id'],
            ref_data['valid_state_province_ids'],
        )
        new_address_ids = addresses_df['ADDRESSID'].tolist() if not addresses_df.empty else []

        # ── Customers ─────────────────────────────────────────────────────────
        customers_df = generate_customers(
            n_customers,
            ref_data['max_customer_id'],
            ref_data['uncustomered_person_ids'],
            new_person_ids,
            ref_data['valid_territory_ids'],
        )
        new_customer_ids = customers_df['CUSTOMERID'].tolist() if not customers_df.empty else []

        # ── Products ──────────────────────────────────────────────────────────
        products_df = generate_products(
            n_products,
            ref_data['max_product_id'],
            ref_data['valid_subcategory_ids'],
        )

        # Merge new product prices into the reference dict so orders can reference
        # products generated in the same run.
        if not products_df.empty:
            new_prices = dict(zip(products_df['PRODUCTID'], products_df['LISTPRICE']))
            ref_data['product_prices'].update(new_prices)

        # ── Sales Orders ──────────────────────────────────────────────────────
        # Address pool includes both existing addresses and those generated this run.
        all_address_ids = ref_data['valid_address_ids'] + new_address_ids

        headers_df, details_df = generate_sales_orders(
            n_headers=n_orders,
            max_order_id=ref_data['max_sales_order_id'],
            max_detail_id=ref_data['max_sales_order_detail_id'],
            valid_customer_ids=ref_data['valid_customer_ids'],
            new_customer_ids=new_customer_ids,
            valid_address_ids=all_address_ids,
            valid_ship_method_ids=ref_data['valid_ship_method_ids'],
            valid_territory_ids=ref_data['valid_territory_ids'],
            product_prices=ref_data['product_prices'],
        )

        return {
            'persons':        persons_df.to_dict('records'),
            'addresses':      addresses_df.to_dict('records'),
            'customers':      customers_df.to_dict('records'),
            'products':       products_df.to_dict('records'),
            'order_headers':  headers_df.to_dict('records'),
            'order_details':  details_df.to_dict('records'),
        }

    # ── Load tasks ────────────────────────────────────────────────────────────
    # Each task receives the full generated data dict and loads only its slice.
    # All six tasks run independently after generate_all_data completes.

    @task(task_id='load_persons')
    def load_persons(data: dict):
        _load_table(
            data['persons'], 'PERSON', 'PERSON',
            sample_cols=['BUSINESSENTITYID', 'FIRSTNAME', 'LASTNAME', 'TITLE', 'EMAILPROMOTION'],
        )

    @task(task_id='load_addresses')
    def load_addresses(data: dict):
        _load_table(
            data['addresses'], 'PERSON', 'ADDRESS',
            sample_cols=['ADDRESSID', 'ADDRESSLINE1', 'CITY', 'STATEPROVINCEID', 'POSTALCODE'],
        )

    @task(task_id='load_customers')
    def load_customers(data: dict):
        _load_table(
            data['customers'], 'SALES', 'CUSTOMER',
            sample_cols=['CUSTOMERID', 'PERSONID', 'TERRITORYID'],
        )

    @task(task_id='load_products')
    def load_products(data: dict):
        _load_table(
            data['products'], 'PRODUCTION', 'PRODUCT',
            sample_cols=['PRODUCTID', 'NAME', 'PRODUCTNUMBER', 'LISTPRICE', 'STANDARDCOST'],
        )

    @task(task_id='load_order_headers')
    def load_order_headers(data: dict):
        _load_table(
            data['order_headers'], 'SALES', 'SALESORDERHEADER',
            sample_cols=['SALESORDERID', 'CUSTOMERID', 'SUBTOTAL', 'TAXAMT', 'ORDERDATE'],
        )

    @task(task_id='load_order_details')
    def load_order_details(data: dict):
        _load_table(
            data['order_details'], 'SALES', 'SALESORDERDETAIL',
            sample_cols=['SALESORDERID', 'SALESORDERDETAILID', 'PRODUCTID', 'ORDERQTY', 'UNITPRICE'],
        )

    @task.branch(task_id='route_persons_load')
    def route_persons_load(data: dict) -> str:
        return 'load_persons' if data['persons'] else 'skip_load_persons'

    @task(task_id='skip_load_persons')
    def skip_load_persons():
        logger.info("Skipping PERSON.PERSON load because no person records were generated.")

    @task.branch(task_id='route_addresses_load')
    def route_addresses_load(data: dict) -> str:
        return 'load_addresses' if data['addresses'] else 'skip_load_addresses'

    @task(task_id='skip_load_addresses')
    def skip_load_addresses():
        logger.info("Skipping PERSON.ADDRESS load because no address records were generated.")

    @task.branch(task_id='route_customers_load')
    def route_customers_load(data: dict) -> str:
        return 'load_customers' if data['customers'] else 'skip_load_customers'

    @task(task_id='skip_load_customers')
    def skip_load_customers():
        logger.info("Skipping SALES.CUSTOMER load because no customer records were generated.")

    @task.branch(task_id='route_products_load')
    def route_products_load(data: dict) -> str:
        return 'load_products' if data['products'] else 'skip_load_products'

    @task(task_id='skip_load_products')
    def skip_load_products():
        logger.info("Skipping PRODUCTION.PRODUCT load because no product records were generated.")

    @task.branch(task_id='route_order_headers_load')
    def route_order_headers_load(data: dict) -> str:
        return 'load_order_headers' if data['order_headers'] else 'skip_load_order_headers'

    @task(task_id='skip_load_order_headers')
    def skip_load_order_headers():
        logger.info("Skipping SALES.SALESORDERHEADER load because no order headers were generated.")

    @task.branch(task_id='route_order_details_load')
    def route_order_details_load(data: dict) -> str:
        return 'load_order_details' if data['order_details'] else 'skip_load_order_details'

    @task(task_id='skip_load_order_details')
    def skip_load_order_details():
        logger.info("Skipping SALES.SALESORDERDETAIL load because no order detail records were generated.")

    # ── Wiring ────────────────────────────────────────────────────────────────
    ref_data_output  = fetch_reference_data()
    generated        = generate_all_data(ref_data_output)

    load_persons_task = load_persons(generated)
    skip_load_persons_task = skip_load_persons()
    route_persons_load(generated) >> [load_persons_task, skip_load_persons_task]

    load_addresses_task = load_addresses(generated)
    skip_load_addresses_task = skip_load_addresses()
    route_addresses_load(generated) >> [load_addresses_task, skip_load_addresses_task]

    load_customers_task = load_customers(generated)
    skip_load_customers_task = skip_load_customers()
    route_customers_load(generated) >> [load_customers_task, skip_load_customers_task]

    load_products_task = load_products(generated)
    skip_load_products_task = skip_load_products()
    route_products_load(generated) >> [load_products_task, skip_load_products_task]

    load_order_headers_task = load_order_headers(generated)
    skip_load_order_headers_task = skip_load_order_headers()
    route_order_headers_load(generated) >> [load_order_headers_task, skip_load_order_headers_task]

    load_order_details_task = load_order_details(generated)
    skip_load_order_details_task = skip_load_order_details()
    route_order_details_load(generated) >> [load_order_details_task, skip_load_order_details_task]


generate_dag()
