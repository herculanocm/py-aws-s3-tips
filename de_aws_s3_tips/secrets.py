from botocore.exceptions import ClientError
import base64

def get_secret(session, secret_name):
    # Create a Secrets Manager client
    client = session.client(service_name='secretsmanager')

    try:
        # Retrieve the secret value
        response = client.get_secret_value(SecretId=secret_name)

        if 'SecretString' in response:
            # For a secret containing only string key-value pairs
            return response['SecretString']
        else:
            # For a secret containing binary data
            return base64.b64decode(response['SecretBinary'])
    
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"The secret with name '{secret_name}' was not found.")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'DecryptionFailure':
            print("Secrets Manager can't decrypt the protected secret text using the provided KMS key.")
        elif e.response['Error']['Code'] == 'InternalServiceError':
            print("An error occurred on the server side.")
        else:
            print("Error:", e)