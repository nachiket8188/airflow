import uuid
import random
import logging
from datetime import datetime

import pandas as pd
from faker import Faker

logger = logging.getLogger(__name__)
fake = Faker()

# Weighted toward None so most persons have no title — mirrors real AdventureWorks distribution
_TITLES = [None, None, None, None, 'Mr.', 'Ms.', 'Mrs.', 'Dr.']

# EmailPromotion: 0 = no contact, 1 = this company only, 2 = third-party ok
# Weighted toward 0 (most people opt out)
_EMAIL_PROMO_OPTIONS = [0, 0, 0, 1, 1, 2]


def generate_persons(n: int, max_business_entity_id: int) -> pd.DataFrame:
    """
    Generates n Person records of PersonType 'IN' (Individual Customer).
    New BusinessEntityIDs are assigned sequentially from max_business_entity_id + 1.

    Args:
        n: Number of Person records to generate.
        max_business_entity_id: Current maximum BUSINESSENTITYID in Snowflake.

    Returns:
        DataFrame with uppercased column names matching the PERSON.PERSON Snowflake table.
    """
    if n == 0:
        logger.info("n_persons is 0. Skipping Person generation.")
        return pd.DataFrame()

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    records = []

    for i in range(1, n + 1):
        records.append({
            'BUSINESSENTITYID': max_business_entity_id + i,
            'PERSONTYPE':       'IN',
            'NAMESTYLE':        False,
            'TITLE':            random.choice(_TITLES),
            'FIRSTNAME':        fake.first_name(),
            # Middle name: mostly absent, occasionally a single initial
            'MIDDLENAME':       random.choice([None, None, None, fake.first_name()[0] + '.']),
            'LASTNAME':         fake.last_name(),
            'SUFFIX':           None,
            'EMAILPROMOTION':   random.choice(_EMAIL_PROMO_OPTIONS),
            'ADDITIONALCONTACTINFO': None,
            'DEMOGRAPHICS':     None,
            'ROWGUID':          str(uuid.uuid4()),
            'MODIFIEDDATE':     now,
        })

    df = pd.DataFrame(records)
    logger.info(
        f"Generated {len(df)} Person records. Sample:\n"
        f"{df[['BUSINESSENTITYID', 'FIRSTNAME', 'LASTNAME', 'EMAILPROMOTION']].head(5).to_string()}"
    )
    return df


def generate_addresses(n: int, max_address_id: int, valid_state_province_ids: list) -> pd.DataFrame:
    """
    Generates n Address records.
    One address per new Person — same count as generate_persons is called with.
    New AddressIDs are assigned sequentially from max_address_id + 1.

    Args:
        n: Number of Address records to generate.
        max_address_id: Current maximum ADDRESSID in Snowflake.
        valid_state_province_ids: List of valid STATEPROVINCEID values available in Snowflake.

    Returns:
        DataFrame with uppercased column names matching the PERSON.ADDRESS Snowflake table.
    """
    if n == 0:
        logger.info("n_persons is 0. Skipping Address generation.")
        return pd.DataFrame()

    if not valid_state_province_ids:
        logger.warning("No valid StateProvinceIDs found in Snowflake. Address generation may fail FK checks.")

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    records = []

    for i in range(1, n + 1):
        records.append({
            'ADDRESSID':        max_address_id + i,
            'ADDRESSLINE1':     fake.street_address(),
            # AddressLine2 is optional — weighted toward absent
            'ADDRESSLINE2':     random.choice([None, None, None, fake.secondary_address()]),
            'CITY':             fake.city(),
            'STATEPROVINCEID':  random.choice(valid_state_province_ids),
            'POSTALCODE':       fake.zipcode()[:15],   # column is varchar(15) in AdventureWorks
            'SPATIALLOCATION':  None,
            'ROWGUID':          str(uuid.uuid4()),
            'MODIFIEDDATE':     now,
        })

    df = pd.DataFrame(records)
    logger.info(
        f"Generated {len(df)} Address records. Sample:\n"
        f"{df[['ADDRESSID', 'ADDRESSLINE1', 'CITY', 'STATEPROVINCEID', 'POSTALCODE']].head(5).to_string()}"
    )
    return df
