## Make sure that most of the packages needed can be imported

import sys
import pytz
from datetime import datetime, timedelta

# Used by DynamoDB Helper
import boto3
from boto3.dynamodb.conditions import Key

# Spark functions used in main()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.functions import when, floor as spark_floor
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import max as spark_max
from pyspark.sql import Window

# Sentiment Analysis Pipeline
from bs4 import BeautifulSoup
from transformers import pipeline
from multiprocessing import cpu_count
from multiprocessing import Pool


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


def get_time_string(timezone_str='US/Eastern'):
    
    timezone = pytz.timezone(timezone_str)
    fmt = '%Y-%m-%d %H:%M:%S %Z%z'

    return datetime.now().astimezone(timezone).strftime(fmt)


def main(args):
    """
    Main function when used as standalone spark-submit script
    """

    ### 1. Load the news aticle data from DynamoDB
    dynamo = DynamoDBHelper()
    
    data_table = 'test_data'
    analysis_window = '2020-07-19_Hour=15'
    news_data = dynamo.read_table(table_name=data_table,
                                  target_analysis_window=analysis_window)

    # TODO use logger
    print('@@@Loaded DynamoDB table')    

    spark = (SparkSession.builder
                        .appName("SparkTest")  # Set app name
                        .getOrCreate())

    print('@@@Started Spark Session')
    print('@@@ Configurations @@@')
    print(spark.sparkContext._conf.getAll())


    ### 3. Parallelize the data to Spark Nodes

    # Convert list to RDD
    news_rdd = spark.sparkContext.parallelize(news_data)

    # Create data frame
    news_df = spark.createDataFrame(news_rdd)

    print(news_df.count())

    # Add a timestamp in a test dynamodb table
    time_string = get_time_string(timezone_str='US/Pacific')
    item_dict = {'test_key': time_string, 'success': 'success'}

    print('Writing timestamp to DynamoDB')
    dynamo.write_item(table_name='airflow_emr_test', item_key_value_dict=item_dict)

if __name__ == "__main__":
    main(sys.argv[1:])

