## Make sure that most of the packages needed can be imported

import sys

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

    def write_item(self, table_name, item):
        target_table = self.dynamodb_conn.Table(table_name)
        response = target_table.put_item(Item=item)
        response_code = response['ResponseMetadata']['HTTPStatusCode']


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

    # Enough for basic test


if __name__ == "__main__":
    main(sys.argv[1:])

