import boto3
from boto3.dynamodb.conditions import Key

import json


def lambda_handler(event, context):
    dynamodb_conn = boto3.resource('dynamodb', region_name='us-west-2')

    table_name = 'public_results_json'
    table = dynamodb_conn.Table(table_name)

    response = table.query(KeyConditionExpression=Key('analysis_window').eq('latest'))

    # Dictionary representation of the JSON data
    news_data = response['Items'][0]

    latest_json = news_data['json_string']

    return latest_json
