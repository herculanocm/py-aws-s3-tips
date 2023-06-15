import boto3
import logging

def assume_role(sts_client,role_arn, session_name, logger=None):
    logger = logging.getLogger()

    logger.info('Assume the role')
    # Assume the role
    response = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name
    )

    logger.info('Retrieve the temporary credentials')
    # Retrieve the temporary credentials
    access_key_id = response['Credentials']['AccessKeyId']
    secret_access_key = response['Credentials']['SecretAccessKey']
    session_token = response['Credentials']['SessionToken']

    logger.info('Create a new session using the temporary credentials')
    # Create a new session using the temporary credentials
    session = boto3.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        aws_session_token=session_token
    )

    return session