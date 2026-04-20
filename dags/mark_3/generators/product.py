import uuid
import random
import logging
from datetime import datetime

import pandas as pd
from faker import Faker

logger = logging.getLogger(__name__)
fake = Faker()

# Reference values matching AdventureWorks domain constraints
_COLORS = [None, 'Black', 'Silver', 'Red', 'White', 'Blue', 'Yellow', 'Grey', 'Multi']
_PRODUCT_LINES = [None, 'R ', 'S ', 'T ', 'M ']   # Road, Sport, Touring, Mountain (2-char with trailing space)
_CLASSES = [None, 'L ', 'M ', 'H ']                # Low, Medium, High
_STYLES = [None, 'U ', 'M ', 'W ']                 # Unisex, Men, Women
_PREFIXES = ['BK', 'SO', 'FR', 'HB', 'HL', 'LL', 'ML', 'PD', 'RB', 'WB']


def _generate_product_number(product_id: int) -> str:
    """Generates a product number in the AdventureWorks format e.g. 'BK-R19B-52'."""
    prefix = random.choice(_PREFIXES)
    mid = fake.bothify(text='?##?').upper()
    return f"{prefix}-{mid}-{product_id:02d}"


def generate_products(n: int, max_product_id: int, valid_subcategory_ids: list) -> pd.DataFrame:
    """
    Generates n Product records with realistic pricing, names, and attributes.
    ListPrice and StandardCost are randomly generated with a realistic margin ratio.
    ProductSubcategoryID is assigned from the existing subcategory list if available.

    Args:
        n: Number of Product records to generate.
        max_product_id: Current maximum PRODUCTID in Snowflake.
        valid_subcategory_ids: List of valid PRODUCTSUBCATEGORYID values.

    Returns:
        DataFrame with uppercased column names matching the PRODUCTION.PRODUCT Snowflake table.
    """
    if n == 0:
        logger.info("n_products is 0. Skipping Product generation.")
        return pd.DataFrame()

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    records = []

    for i in range(1, n + 1):
        product_id = max_product_id + i
        list_price = round(random.uniform(5.0, 3000.0), 4)
        # Standard cost is 40-75% of list price — realistic manufacturing margin
        standard_cost = round(list_price * random.uniform(0.40, 0.75), 4)

        records.append({
            'PRODUCTID':              product_id,
            'NAME':                   fake.bs().title()[:50],      # varchar(50)
            'PRODUCTNUMBER':          _generate_product_number(product_id),
            'MAKEFLAG':               True,
            'FINISHEDGOODSFLAG':      True,
            'COLOR':                  random.choice(_COLORS),
            'SAFETYSTOCKLEVEL':       random.randint(100, 1000),
            'REORDERPOINT':           random.randint(50, 500),
            'STANDARDCOST':           standard_cost,
            'LISTPRICE':              list_price,
            'SIZE':                   random.choice([None, 'S', 'M', 'L', 'XL', '38', '40', '42', '44']),
            'SIZEUNITMEASURECODE':    None,
            'WEIGHTUNITMEASURECODE':  None,
            'WEIGHT':                 None,
            'DAYSTOMANUFACTURE':      random.randint(0, 4),
            'PRODUCTLINE':            random.choice(_PRODUCT_LINES),
            'CLASS':                  random.choice(_CLASSES),
            'STYLE':                  random.choice(_STYLES),
            'PRODUCTSUBCATEGORYID':   random.choice(valid_subcategory_ids) if valid_subcategory_ids else None,
            'PRODUCTMODELID':         None,
            'SELLSTARTDATE':          now,
            'SELLENDDATE':            None,
            'DISCONTINUEDDATE':       None,
            'ROWGUID':                str(uuid.uuid4()),
            'MODIFIEDDATE':           now,
        })

    df = pd.DataFrame(records)
    logger.info(
        f"Generated {len(df)} Product records. Sample:\n"
        f"{df[['PRODUCTID', 'NAME', 'PRODUCTNUMBER', 'LISTPRICE', 'STANDARDCOST']].head(5).to_string()}"
    )
    return df
