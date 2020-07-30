import json
import pandas as pd

import boto3
from boto3.dynamodb.conditions import Key

from datetime import datetime, timedelta

def json_from_analysis_window(analysis_window):

    # Get the current datetime object
    today_str = analysis_window[:10]
    today_dt = datetime.strptime(today_str, '%Y-%m-%d')

    # Initialize DynamoDB connection
    table_name  = 'test_results'
    index_name = 'analysis_date_index'

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # Store a list of dataframes with the sentiment label and scores for each stock and each day.
    sentiment_df_list = []

    # Past 7 days includes today.
    for i in range(7):
        current_dt = today_dt - timedelta(days=i)
        
        # Use global secondary index to query data for a given day only.
        current_str = current_dt.strftime('%Y-%m-%d')
        response = table.query(
                    IndexName=index_name,
                    KeyConditionExpression=Key('analysis_date').eq(current_str)
                    )
        
        # Columns: analysis_date, analysis_window, api_success_e_str, final_score, label, t_symb
        current_date_df = pd.DataFrame(response['Items'])

        # Get the date string for the most latest hour
        latest_hours_df = current_date_df.groupby('t_symb').agg({'analysis_window':'max'}).reset_index()

        # Fetch subset of data for the most latest hour of that day
        subset_hour_df = pd.DataFrame.merge(current_date_df, latest_hours_df, on=['t_symb','analysis_window'], how='inner')

        # Get the date, ticker symbol, api_success_est_time and predicted sentiment label and score
        current_day_sentiment_df = subset_hour_df[['analysis_date','t_symb', 'label', 'final_score','api_success_e_str']]
        
        # Add a categorical column to identify 1st (today) to 7th (furthest in the past) day 
        # using labels day-1 to day-7
        current_day_sentiment_df = current_day_sentiment_df.assign(day_label=f'day-{i+1}')
        
        sentiment_df_list.append(current_day_sentiment_df)

    # Combine the list of dataframes into one long dataframe
    sentiment_7_days_df = pd.concat(sentiment_df_list,axis=0)

    # Rename label to sentiment label
    sentiment_7_days_df = sentiment_7_days_df.rename(columns={'label':'sentiment_label'})

    sentiment_7_days_df = sentiment_7_days_df.assign(month_day=sentiment_7_days_df
                                            .analysis_date
                                            .apply(gen_month_day_string))

    # Chop off the 4 digit timezone info in last updated e.g. EDT-0400 to EDT
    sentiment_7_days_df = sentiment_7_days_df.assign(last_updated = sentiment_7_days_df
                                            .api_success_e_str
                                            .map(lambda x: x[:-5]))

    color_map = {'POSITIVE':'#1a9641', 'NEGATIVE':'#d7191c', 'UNCERTAIN':'#607d8b'}
    sentiment_7_days_df = sentiment_7_days_df.assign(hex_color=sentiment_7_days_df
                                                                    .sentiment_label
                                                                    .map(color_map))

    # Create nested dictionary from pandas dataframe with t_symb as first level key 
    # and day_label as second level key
    # https://stackoverflow.com/questions/41998624/how-to-convert-pandas-dataframe-to-nested-dictionary
    nested_7_days = (sentiment_7_days_df.groupby('t_symb')[['day_label','sentiment_label',
                                                            'analysis_date','month_day','hex_color',
                                                            'last_updated']]
                                    .apply(lambda x: x.set_index('day_label').to_dict(orient='index'))
                                    .to_dict()
                    )

    # Serialize dictionary to a JSON formatted string.
    json_string = json.dumps(nested_7_days)  # , indent=4) # If writting to file.

    return json_string


# Convert the date string to a "month/day" string
def gen_month_day_string(input_date_string):
    m_day = input_date_string[-5:]
    month, day = m_day.split('-')
    return f'{month}/{day}'


if __name__ == "__main__":
    # Test analysis window
    analysis_window = '2020-07-25_Hour=15'
    json_from_analysis_window(analysis_window)
