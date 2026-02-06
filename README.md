# Airflow ETL Pipelines Practice

## Description

    This repo is used to house the ETL scripts I've written using Airflow. These scripts will move data between different
    sources like S3 bucket, Snowflake DW, on-prem SQL-Server instance and so on. The scripts will use multiple tools like
    AWS Glue, Snowpark etc. to process data wherever required.

## Steps for setup

Following steps are the things I did on my local Windows machine to set the environment up.

    1. Download and build from Airflow's official Docker Image (mounting volumes by modifying docker-compose.yaml)
    2. Export the Python environment from Docker container using `pip freeze` to be able to re-create the environment
       locally (for utilizing code auto-completion and other features of IDE)
    3. Create a replica of the Python environment from within Docker container, locally, using `conda` package manager
       and following command:
        `pip install "apache-airflow[amazon,postgres, snowflake]==2.10.5" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.9.txt"`
    4. Create AWS Trial account for free access to services like S3, Glue etc.
    5. Install AWS-CLI to enable running of scripts for various purposes.
    6. Configure AWS Keys using AWS CLI. The final version of DAG mark_1 relies on credentials/keys configured using 
       AWS CLI tool (such as region, user etc.) and role ARN is specified in .env file (not published).
    7. Create Snowflake Trial account. Snowflake connection parameters are specified in .env file.
    8. Configure Git and GitHub with .gitignore, .gitkeep (initially I had empty directories that I wanted tracked)
    9. 

## Current POCs (DAGs)

This repo will eventually contain multiple Airflow + Snowflake (and possibly dbt) POCs. For now, there are two:

1. **Mark 1: S3 to Snowflake**  
   DAG file: `./dags/mark_1/s3_to_snowflake.py`  
   Summary: Downloads a CSV from S3, aggregates sales by product category with pandas, and loads the results into Snowflake.

2. **Mark 2: SQL Server to Snowflake**  
   DAG file: `./dags/mark_2/sqlserver_to_snowflake.py`  
   Summary: Parameterized DAG that selects a SQL Server table, validates target row count in Snowflake, then extracts and loads via `write_pandas`.
