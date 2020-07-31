import json
from dynamodb_helper import DynamoDBHelper
from time_helper import get_time_string

def lambda_handler(event, context):

    time_string = get_time_string(timezone_str='US/Pacific')
    item_dict = {'test_key': time_string, 'success': 'success'}

    dynamodb = DynamoDBHelper()
    dynamodb.write_item(table_name='airflow_lambda_test', item_key_value_dict=item_dict)

    return {
        'statusCode': 200,
        'body': 'Success'
    }
