# Mark 3: Faker to Snowflake DAG

**Full disclosure : Completely AI generated DAG.**

This DAG (`faker_to_snowflake`) simulates daily OLTP-style activity for an AdventureWorks-like retail model and loads the generated records into Snowflake. It creates data for `Person`, `Address`, `Customer`, `Product`, `SalesOrderHeader`, and `SalesOrderDetail`, using the same `write_pandas` loading mechanism used elsewhere in the project.

## How It Works

Before generating any new rows, the DAG queries Snowflake for the current primary-key high-water marks and valid foreign-key values. That reference data is then used to generate realistic records while preserving referential integrity across related entities.

The row counts are controlled through Airflow trigger parameters, so each run can generate different volumes of data without changing code. Generated DataFrames are converted to JSON-safe record dictionaries for XCom transport, then reconstructed and loaded into Snowflake in downstream tasks.

## Notes

- `schedule=None`: the DAG is intended for manual triggering during development and testing.
- `SalesOrderDetail` rows are generated with variable line counts per order, so the total detail volume changes naturally from run to run.
- For the default settings, the XCom payload size should remain within normal Airflow limits, but significantly larger runs may need a different XCom backend.
