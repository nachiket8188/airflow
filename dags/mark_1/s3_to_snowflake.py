import boto3
import sys

from etl.aws import fetch_parameter, assume_role

s3_location_parameter_name = "/retail_sales/bucket_prefix"
aws_role_parameter_name = "/airflow/airflow_user"

s3_bucket = fetch_parameter(s3_location_parameter_name)
print(f"S3 Bucket Name : {s3_bucket}")

aws_role_arn = "arn:aws:iam::XXX:role/airflow_s3_glue_user_role"
assumed_session = assume_role(role_arn=aws_role_arn)

if s3_bucket is None:
    sys.exit("Exiting. Something went wrong. Check the logs.")
else:
    s3_client = assumed_session.client("s3")
    response = s3_client.list_objects_v2(Bucket=s3_bucket)
    file_list = [file["Key"] for file in response["Contents"]]
    print(file_list)
