import uuid
import random
import string
import logging
from datetime import datetime, timedelta

import pandas as pd

logger = logging.getLogger(__name__)

# Weighted distribution for number of detail lines per order.
# Most real orders have 1-4 items; a few are larger basket orders.
# Index position = line item count, value = relative weight.
_LINE_ITEM_COUNTS = list(range(1, 9))          # 1 through 8 lines
_LINE_ITEM_WEIGHTS = [20, 25, 20, 15, 10, 5, 3, 2]   # sums to 100

# Discount rates — most orders have no discount
_DISCOUNT_OPTIONS = [0.0, 0.0, 0.0, 0.0, 0.05, 0.10, 0.15]


def _generate_tracking_number() -> str:
    """Generates a carrier tracking number in the format used by AdventureWorks: XXXX-XXXX-XXXXXXXX."""
    seg1 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    seg2 = ''.join(random.choices(string.digits, k=4))
    seg3 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
    return f"{seg1}-{seg2}-{seg3}"


def generate_sales_orders(
    n_headers: int,
    max_order_id: int,
    max_detail_id: int,
    valid_customer_ids: list,
    new_customer_ids: list,
    valid_address_ids: list,
    valid_ship_method_ids: list,
    valid_territory_ids: list,
    product_prices: dict,
) -> tuple:
    """
    Generates SalesOrderHeader and SalesOrderDetail records together.
    Detail lines are generated per header using a weighted random line-item count (1-8),
    so the total detail rows will vary naturally across runs rather than being a fixed multiple.

    SubTotal on the header is computed from the sum of (Qty * UnitPrice * (1 - Discount))
    across all its detail lines, matching how AdventureWorks computes TotalDue.

    Args:
        n_headers: Number of SalesOrderHeader records to generate.
        max_order_id: Current maximum SALESORDERID in Snowflake.
        max_detail_id: Current maximum SALESORDERDETAILID in Snowflake.
        valid_customer_ids: Existing CustomerIDs from Snowflake.
        new_customer_ids: CustomerIDs generated in the current run (not yet in Snowflake).
        valid_address_ids: All valid AddressIDs (existing + new from current run).
        valid_ship_method_ids: Valid ShipMethodIDs from Purchasing.ShipMethod.
        valid_territory_ids: Valid TerritoryIDs from Sales.SalesTerritory.
        product_prices: Dict of {product_id: list_price} for all orderable products.

    Returns:
        Tuple of (headers_df, details_df). Both DataFrames have uppercased column names
        matching their respective Snowflake tables.
    """
    if n_headers == 0:
        logger.info("n_orders is 0. Skipping SalesOrder generation.")
        return pd.DataFrame(), pd.DataFrame()

    all_customer_ids = valid_customer_ids + new_customer_ids
    all_product_ids = list(product_prices.keys())

    if not all_customer_ids:
        logger.warning("No customer IDs available. Cannot generate SalesOrderHeaders.")
        return pd.DataFrame(), pd.DataFrame()

    if not all_product_ids:
        logger.warning("No products with a list price found. Cannot generate SalesOrderDetails.")
        return pd.DataFrame(), pd.DataFrame()

    if not valid_address_ids:
        logger.warning("No address IDs available. BillTo/ShipTo addresses will be NULL.")

    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S.%f')
    header_records = []
    detail_records = []
    current_detail_id = max_detail_id

    for i in range(1, n_headers + 1):
        order_id = max_order_id + i

        # Order date is within the last 30 days; due date is 7 days after; ship date 1-5 days after order
        order_date = now - timedelta(days=random.randint(0, 30))
        due_date   = order_date + timedelta(days=7)
        ship_date  = order_date + timedelta(days=random.randint(1, 5))

        customer_id    = random.choice(all_customer_ids)
        bill_address   = random.choice(valid_address_ids) if valid_address_ids else None
        ship_address   = random.choice(valid_address_ids) if valid_address_ids else None
        territory_id   = random.choice(valid_territory_ids) if valid_territory_ids else None
        ship_method_id = random.choice(valid_ship_method_ids) if valid_ship_method_ids else 1

        # ── Generate detail lines ─────────────────────────────────────────────
        n_lines = random.choices(_LINE_ITEM_COUNTS, weights=_LINE_ITEM_WEIGHTS, k=1)[0]
        # Allow the same product to appear once per order at most (realistic)
        selected_products = random.sample(all_product_ids, min(n_lines, len(all_product_ids)))

        subtotal = 0.0
        for product_id in selected_products:
            current_detail_id += 1
            qty        = random.randint(1, 5)
            unit_price = product_prices[product_id]
            discount   = random.choice(_DISCOUNT_OPTIONS)
            line_total = round(qty * unit_price * (1 - discount), 4)
            subtotal  += line_total

            detail_records.append({
                'SALESORDERID':          order_id,
                'SALESORDERDETAILID':    current_detail_id,
                'CARRIERTRACKINGNUMBER': _generate_tracking_number(),
                'ORDERQTY':              qty,
                'PRODUCTID':             product_id,
                'SPECIALOFFERID':        1,    # 1 = No Discount — always present in AdventureWorks
                'UNITPRICE':             unit_price,
                'UNITPRICEDISCOUNT':     discount,
                'ROWGUID':               str(uuid.uuid4()),
                'MODIFIEDDATE':          now_str,
            })

        # ── Compute header financials from detail lines ───────────────────────
        subtotal  = round(subtotal, 4)
        tax_amt   = round(subtotal * 0.08, 4)   # 8% tax — consistent with AW baseline data
        freight   = round(subtotal * 0.02, 4)   # 2% freight

        header_records.append({
            'SALESORDERID':            order_id,
            'REVISIONNUMBER':          8,
            'ORDERDATE':               order_date.strftime('%Y-%m-%d %H:%M:%S.%f'),
            'DUEDATE':                 due_date.strftime('%Y-%m-%d %H:%M:%S.%f'),
            'SHIPDATE':                ship_date.strftime('%Y-%m-%d %H:%M:%S.%f'),
            'STATUS':                  5,      # 5 = Shipped
            'ONLINEORDERFLAG':         True,
            'PURCHASEORDERNUMBER':     None,
            'ACCOUNTNUMBER':           None,
            'CUSTOMERID':              customer_id,
            'SALESPERSONID':           None,   # online orders have no assigned salesperson
            'TERRITORYID':             territory_id,
            'BILLTOADDRESSID':         bill_address,
            'SHIPTOADDRESSID':         ship_address,
            'SHIPMETHODID':            ship_method_id,
            'CREDITCARDID':            None,
            'CREDITCARDAPPROVALCODE':  None,
            'CURRENCYRATEID':          None,
            'SUBTOTAL':                subtotal,
            'TAXAMT':                  tax_amt,
            'FREIGHT':                 freight,
            'COMMENT':                 None,
            'ROWGUID':                 str(uuid.uuid4()),
            'MODIFIEDDATE':            now_str,
        })

    headers_df = pd.DataFrame(header_records)
    details_df = pd.DataFrame(detail_records)

    logger.info(
        f"Generated {len(headers_df)} SalesOrderHeader records "
        f"with {len(details_df)} total SalesOrderDetail lines "
        f"(avg {len(details_df) / len(headers_df):.1f} lines/order)."
    )
    logger.info(
        f"Header sample:\n"
        f"{headers_df[['SALESORDERID', 'CUSTOMERID', 'SUBTOTAL', 'TAXAMT', 'ORDERDATE']].head(5).to_string()}"
    )
    logger.info(
        f"Detail sample:\n"
        f"{details_df[['SALESORDERID', 'SALESORDERDETAILID', 'PRODUCTID', 'ORDERQTY', 'UNITPRICE']].head(5).to_string()}"
    )
    return headers_df, details_df
