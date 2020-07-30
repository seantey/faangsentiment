import json
from news_api_handler import *

# https://towardsdatascience.com/introduction-to-amazon-lambda-layers-and-boto3-using-python3-39bd390add17
# https://stackoverflow.com/questions/48912253/aws-lambda-unable-to-import-module-lambda-function-no-module-named-requests


def lambda_handler(event, context):
    analysis_window = event.get('analysis_window')
    update_FAANG_yahoo_news(current_analysis_group=analysis_window)

    return {
        'statusCode': 200,
        'body': json.dumps(f'Hello from Lambda! WINDOW~{analysis_window}')
    }
