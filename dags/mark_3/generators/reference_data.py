import logging
import snowflake.connector

logger = logging.getLogger(__name__)


def fetch_all_reference_data(conn: snowflake.connector.SnowflakeConnection) -> dict:
    """
    Queries Snowflake for all IDs and reference values needed to maintain referential integrity
    when generating new Faker records. Called once at DAG start before any generation happens.

    Returns a dict with:
      - max_* keys: current maximum PK values per table (used to assign new IDs sequentially)
      - valid_* keys: lists of existing FK values (used to satisfy foreign key constraints)
      - product_prices: dict of {product_id: list_price} for seeding SalesOrderDetail unit prices
      - uncustomered_person_ids: persons not yet linked to a Sales.Customer record
    """
    cursor = conn.cursor()
    ref = {}

    # ── Max PKs (for sequential ID assignment in generators) ─────────────────
    max_id_queries = {
        'max_business_entity_id':  "SELECT COALESCE(MAX(BUSINESSENTITYID), 0)  FROM PERSON.PERSON",
        'max_address_id':          "SELECT COALESCE(MAX(ADDRESSID), 0)          FROM PERSON.ADDRESS",
        'max_customer_id':         "SELECT COALESCE(MAX(CUSTOMERID), 0)         FROM SALES.CUSTOMER",
        'max_product_id':          "SELECT COALESCE(MAX(PRODUCTID), 0)          FROM PRODUCTION.PRODUCT",
        'max_sales_order_id':      "SELECT COALESCE(MAX(SALESORDERID), 0)       FROM SALES.SALESORDERHEADER",
        'max_sales_order_detail_id': "SELECT COALESCE(MAX(SALESORDERDETAILID), 0) FROM SALES.SALESORDERDETAIL",
    }

    for key, query in max_id_queries.items():
        cursor.execute(query)
        ref[key] = cursor.fetchone()[0]
        logger.info(f"Fetched {key}: {ref[key]}")

    # ── Valid FK lists (for satisfying foreign key constraints) ───────────────
    # Built only from the subset of Snowflake tables populated for this POC.
    fk_list_queries = {
        'valid_customer_ids': "SELECT CUSTOMERID FROM SALES.CUSTOMER",
        'valid_address_ids':  "SELECT ADDRESSID  FROM PERSON.ADDRESS",
        'valid_territory_ids': "SELECT TERRITORYID FROM SALES.SALESTERRITORY",
        'valid_state_province_ids': """
            SELECT DISTINCT STATEPROVINCEID
            FROM PERSON.ADDRESS
            WHERE STATEPROVINCEID IS NOT NULL
        """,
        'valid_ship_method_ids': """
            SELECT DISTINCT SHIPMETHODID
            FROM SALES.SALESORDERHEADER
            WHERE SHIPMETHODID IS NOT NULL
        """,
        'valid_subcategory_ids': "SELECT PRODUCTSUBCATEGORYID FROM PRODUCTION.PRODUCTSUBCATEGORY",
    }

    for key, query in fk_list_queries.items():
        cursor.execute(query)
        ref[key] = [row[0] for row in cursor.fetchall()]
        logger.info(f"Fetched {key}: {len(ref[key])} records")

    # ── Persons not yet registered as customers ───────────────────────────────
    # Used by the customer generator to avoid creating duplicate Customer records
    # for the same person.
    cursor.execute("""
        SELECT p.BUSINESSENTITYID
        FROM   PERSON.PERSON    p
        LEFT JOIN SALES.CUSTOMER c ON p.BUSINESSENTITYID = c.PERSONID
        WHERE  c.PERSONID IS NULL
    """)
    ref['uncustomered_person_ids'] = [row[0] for row in cursor.fetchall()]
    logger.info(f"Fetched uncustomered_person_ids: {len(ref['uncustomered_person_ids'])} records")

    # ── Product list prices (for seeding SalesOrderDetail unit prices) ────────
    # Only finished goods with a non-zero list price are usable in orders.
    cursor.execute("""
        SELECT PRODUCTID, LISTPRICE
        FROM   PRODUCTION.PRODUCT
        WHERE  LISTPRICE > 0
          AND  FINISHEDGOODSFLAG = TRUE
    """)
    ref['product_prices'] = {row[0]: float(row[1]) for row in cursor.fetchall()}
    logger.info(f"Fetched product_prices: {len(ref['product_prices'])} products")

    cursor.close()
    return ref
