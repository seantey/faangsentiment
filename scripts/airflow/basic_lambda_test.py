import boto3

def basic_lambda_test():
    lambda_client = boto3.client('lambda', region_name='us-west-2')
    response = lambda_client.invoke(
            FunctionName='airflow_test',
            InvocationType='RequestResponse'
    )
    data = response['Payload'].read()
    print(data)