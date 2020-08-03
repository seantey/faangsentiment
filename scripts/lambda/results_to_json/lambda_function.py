import json
from gen_website_json import json_from_analysis_window
from dynamodb_helper import DynamoDBHelper


def lambda_handler(event, context):
    analysis_window = event.get('analysis_window')
    json_string = json_from_analysis_window(analysis_window=analysis_window, read_table_name='news_results')


    # Store as JSON string which should be inserted twice into the table.
    # The first insert should overwrite the data on the key="latest", so that latest data can be easily pulled.
    # The second key is just the current analysis window so that we can keep track of previous public results.

    item1_dict = {'analysis_window':'latest', 'json_string': json_string}
    item2_dict = {'analysis_window':analysis_window, 'json_string': json_string}

    dynamodb = DynamoDBHelper()
    dynamodb.write_item(table_name='results_json', item_key_value_dict=item1_dict)
    dynamodb.write_item(table_name='results_json', item_key_value_dict=item2_dict)

    return {
        'statusCode': 200,
        'body': json_string
    }
