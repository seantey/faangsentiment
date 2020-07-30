import boto3
from boto3.dynamodb.conditions import Key


class DynamoDBHelper:
    def __init__(self):
        self.dynamodb_conn = boto3.resource('dynamodb', region_name='us-west-2')

    def read_table(self, table_name, target_analysis_window):
        table = self.dynamodb_conn.Table(table_name)
        response = table.query(KeyConditionExpression=Key('analysis_window')
                               .eq(target_analysis_window)
                               )

        # This is a list of dictionaries where
        # the keys of each dictionary is a key/column in the DynamoDB table
        news_data = response['Items']

        return news_data

    def write_table(self, table_name, data_dict_list):

        results_table = self.dynamodb_conn.Table(table_name)
        # Use batch writer to automatically handle buffering and sending items in batches
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html
        with results_table.batch_writer() as batch:
            for data_row in data_dict_list:
                batch.put_item(
                    Item=data_row
                )

    def write_item(self, table_name, item_key_value_dict):
        target_table = self.dynamodb_conn.Table(table_name)
        response = target_table.put_item(Item=item_key_value_dict)
        response_code = response['ResponseMetadata']['HTTPStatusCode']
