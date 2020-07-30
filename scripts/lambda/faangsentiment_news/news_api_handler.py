import boto3
import json
import requests

from datetime import datetime
from decimal import Decimal

from aws_secret_manager import *
from time_helper import *

def get_yahoo_stock_news(symbol: str, api_keys: dict) -> dict:
    url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/get-news"

    querystring = {"region": "US", "category": f"{symbol}"}

    headers = {
        'x-rapidapi-host': f"{api_keys['yahoo_api_host']}",
        'x-rapidapi-key': f"{api_keys['yahoo_api_key']}"
        }

    response = requests.request("GET", url, headers=headers, params=querystring)
    
    r_dict = json.loads(response.text)
    
    return r_dict


def put_news_data(news_data: dict, table_name: str='test_data') -> dict:

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    response = table.put_item(Item=news_data)
    
    return response


# TODO allow changing target table
def update_FAANG_yahoo_news(current_analysis_group: str):
    """
    Fetch FAANG related news from APIs and uploads them plus some metadata to DynamoDB.

    :current_analysis_group: The string which defines the current analysis group name.
    """
    stock_symbols = ['FB', 'AMZN', 'AAPL', 'NFLX', 'GOOG']
    api_keys = get_secrets(secret_name='news_api')
    analysis_date = current_analysis_group[:10]
    
    responses = []
    for current_symbol in stock_symbols:
        current_api_source = 'YAHOO'
        
        r_dict = get_yahoo_stock_news(current_symbol, api_keys)

        # Find out how many articles were fetched and count number of digits for padding.
        # Left pad the item counter with zeros: 01, 02, ..., 12, ..., 50
        # Should not really affect sentiment score but rows looks nicer when sorting.
        total_num_articles = len(r_dict['items']['result'])
        digits = len(str(total_num_articles)) # Somewhat lazy method        

        # Time stamp for API call
        # TODO handle API call failure with Apache Airflow
        success_utc_ts = get_time_stamp(timezone_str='Universal')
        success_utc_str = get_time_string(timezone_str='Universal')     
        success_e_str = get_time_string(timezone_str='US/Eastern')
        
        item_counter = 1
        
        for news_data in r_dict['items']['result']:
            yahoo_link = news_data['link']
            news_timestamp = news_data['published_at']
            news_publisher = news_data['publisher']
            news_title = news_data['title']
            news_content = news_data['content']          

            item_counter_str = str(item_counter).rjust(digits, '0')
            
            data_row = {
                'analysis_window': current_analysis_group, # Partition Key
                'symb_counter_source': f'{current_symbol}_{item_counter_str}_api={current_api_source}', # Sort Key
                'analysis_date': analysis_date,
                'source_api': current_api_source,
                't_symb':current_symbol,
                'api_success_utc_ts': Decimal(str(success_utc_ts)),
                'api_success_utc_str': success_utc_str,
                'api_success_e_str': success_e_str,                
                'news_link': yahoo_link,
                'news_timestamp': Decimal(str(news_timestamp)),
                'news_publisher': news_publisher,
                'news_title': news_title,
                'news_content': news_content
            }
            
            response = put_news_data(data_row, 'test_data')
            response_code = response['ResponseMetadata']['HTTPStatusCode']
            responses.append(response_code)
            
            # TODO replace with logger for production code
            print(f'Response code = {response_code}, for storing {current_symbol} news, item # = {item_counter}, with {current_api_source} API')
            item_counter += 1
            