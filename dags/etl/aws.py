import boto3
import logging

logger = logging.getLogger(__name__)

def fetch_parameter(parameter_name:str) -> str:
    """ Fetch a value from AWS Systems Manager Parameter Store """
    try:
        logger.info(f"Fetching parameter : {parameter_name}")
        ssm_client = boto3.client("ssm")
        response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
        return response['Parameter']['Value']

    except ssm_client.exceptions.ParameterNotFound as e:
        logger.info(f"Parameter {parameter_name} not found. {e}")
        return None

def assume_role(role_arn:str, session_name="assumed_role_session") -> boto3.Session:
    """ Assume a role (determined by input ARN) with enough permissions to perform an operation
    and return a boto3 Session instance.
    """
    sts_client = boto3.client("sts") # STS = Security Token Services

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