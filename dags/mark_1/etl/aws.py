import boto3

def fetch_parameter(parameter_name):
    try:
        ssm_client = boto3.client("ssm")
        response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
        return response['Parameter']['Value']

    except ssm_client.exceptions.ParameterNotFound as e:
        print(f"Something went wrong. {e}")
        return None

def assume_role(role_arn, session_name="assumed_role_session"):
    sts_client = boto3.client("sts")

    response = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName=session_name
    )

    credentials = response["Credentials"]
    assumed_session = boto3.Session(
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )

    return assumed_session