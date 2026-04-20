import uuid
import random
import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


def generate_customers(
    n: int,
    max_customer_id: int,
    uncustomered_person_ids: list,
    new_person_ids: list,
    valid_territory_ids: list,
) -> pd.DataFrame:
    """
    Generates n Customer records. Each customer is linked to a Person who does not
    yet have a Customer record, preserving the one-to-one relationship between
    Person.Person and Sales.Customer for individual (non-store) customers.

    The pool of eligible persons consists of:
      - Existing persons not yet linked to any Customer record (fetched from Snowflake)
      - Persons generated in the current DAG run (not yet in Snowflake)

    If the total available pool is smaller than n, generation is capped with a warning.

    Args:
        n: Number of Customer records to generate.
        max_customer_id: Current maximum CUSTOMERID in Snowflake.
        uncustomered_person_ids: Existing PersonIDs not yet linked to a Customer.
        new_person_ids: PersonIDs generated in the current run (not yet in Snowflake).
        valid_territory_ids: List of valid TERRITORYID values from Sales.SalesTerritory.

    Returns:
        DataFrame with uppercased column names matching the SALES.CUSTOMER Snowflake table.
    """
    if n == 0:
        logger.info("n_customers is 0. Skipping Customer generation.")
        return pd.DataFrame()

    available_person_ids = uncustomered_person_ids + new_person_ids

    if not available_person_ids:
        logger.warning("No uncustomered persons available. Cannot generate Customers.")
        return pd.DataFrame()

    if len(available_person_ids) < n:
        logger.warning(
            f"Requested {n} customers but only {len(available_person_ids)} uncustomered persons "
            f"are available. Capping generation at {len(available_person_ids)}."
        )
        n = len(available_person_ids)

    selected_person_ids = random.sample(available_person_ids, n)
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    records = []

    for i, person_id in enumerate(selected_person_ids):
        records.append({
            'CUSTOMERID':   max_customer_id + i + 1,
            'PERSONID':     person_id,
            'STOREID':      None,   # individual customers are not store accounts
            'TERRITORYID':  random.choice(valid_territory_ids) if valid_territory_ids else None,
            'ROWGUID':      str(uuid.uuid4()),
            'MODIFIEDDATE': now,
        })

    df = pd.DataFrame(records)
    logger.info(
        f"Generated {len(df)} Customer records. Sample:\n"
        f"{df[['CUSTOMERID', 'PERSONID', 'TERRITORYID']].head(5).to_string()}"
    )
    return df
