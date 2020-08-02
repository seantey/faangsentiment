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


def main(args):
    """
    Main function when used as standalone spark-submit script
    """

    # TODO write parse args code:
    # add named args processing like --test 
    # check whether it's possible to have named args compatible
    # with running spark-submit command
    # e.g. $ spark-submit spark-script.py 2020-07-25_Hour=00
    # or $ spark-submit spark-script.py --test
    # or throw error when no analysis window given
    test_mode = True
    print(f'@@@@@@@ Arguments: {args}')

    # TODO remove quotes around args

    if test_mode:
        data_table = 'test_data'
        results_table = 'test_results'

        # TODO probably not a good idea to hard code this, even for a test.
        if len(args) == 0:
            analysis_window = '2020-07-25_Hour=00'
        else:
            analysis_window = args[0]

    else:
        data_table = 'news_data'
        results_table = 'news_results'
        analysis_window = args[0]

    process_analysis_window(analysis_window, data_table, results_table)


def process_analysis_window(analysis_window, data_table, results_table):
    """
    Process a given analysis window using Spark for the following steps:

    1. Load the news aticle data from DynamoDB
    2. Start a Spark session if it does not exists
    3. Parallelize the data to Spark Nodes
    4. Process the data from raw text to final sentiment score and label
    5. Store the results into DynamoDB    
    """
    ### 1. Load the news aticle data from DynamoDB
    dynamo = DynamoDBHelper()

    news_data = dynamo.read_table(table_name=data_table,
                                  target_analysis_window=analysis_window)

    # TODO use logger
    print('@@@Loaded DynamoDB table')

    ### 2. Start Spark session

    # TODO maybe move this to main and make the spark context an argument
    # to this function, that way the spark context can be initialized
    # based on commandline arguments such as --local_test
    # if local_test == True:
    #     spark = (SparkSession.builder
    #                         .appName("SparkTest") # Set app name
    #                         .master("local[2]") # Run locally with 2 cores
    #                         .config("spark.driver.memory", "4g")
    #                         .config("spark.executor.memory", "3g")
    #                         .getOrCreate())

    spark = (SparkSession.builder
                        .appName("SparkTest")  # Set app name
                        .getOrCreate())


    # TODO use logger
    print('@@@Started Spark Session')
    print('@@@ Configurations @@@')
    print(spark.sparkContext._conf.getAll())


    ### 3. Parallelize the data to Spark Nodes

    # Convert list to RDD
    news_rdd = spark.sparkContext.parallelize(news_data)

    # Create data frame
    news_df = spark.createDataFrame(news_rdd)

    ### 4. Process the data from raw text to final sentiment score and label

    # TODO use logger
    print('@@@ Initializing Sentiment UDF')

    # Add Sentiment Analysis Pipeline as a UDF to spark
    s_pipe = SentimentAnalysisPipeline()

    udf_schema = StructType([
        StructField("label", StringType(), nullable=False),
        StructField("score", FloatType(), nullable=False)
    ])

    s_pipe_udf = udf(s_pipe.raw_text_to_sentiment, udf_schema)

    # TODO use logger
    print('@@@ Defining transformations')

    # Start defining spark transformations, note that these
    # transformations are lazily evaluated so they are executed
    # only at the end when an action is triggered.

    # Run all news titles through the sentiment pipeline
    # TODO switch to full article analysis with summarizer later on.
    # Drop nulls, most likely there won't be any for titles
    title_df = news_df.select('analysis_window', 'analysis_date', 't_symb', 'news_timestamp', 'news_title').na.drop()

    sentiment_df = title_df.withColumn('sentiment', s_pipe_udf(title_df['news_title']))

    # Subset columns
    sentiment_df = sentiment_df.select('analysis_window', 'analysis_date', 't_symb', 'news_timestamp',
                                    'news_title', 'sentiment.label', 'sentiment.score')

    ## Final Label Calculation:
    # We want a final label of either **POSITIVE**, **NEGATIVE**, or **UNCERTAIN**.

    # We will use a somewhat naive and simple approach to calculating sentiment through averaging.

    # Criteria for final score:
    # * The final score should be between -1 and 1.
    # * The older news, the less important it is, scores are weighed exponentially
    # less every 3 hours from the most recent news.
    # * E.g. Most recent news have a weight of 1 and news that are 3 hours away from
    # the MAX timestamp have a weight of 0.5, 6 hour away from MAX timestamp has weight of 0.25 and so on.
    # * Any score between -0.7 and 0.7 (exclusive) is labelled UNCERTAIN
    # * Scores that are 0.7 or greater are labelled POSITIVE
    # * Scores that are -0.7 or less are labelled NEGATIVE

    ## Scores are bounded between -1 and 1
    # If the label is NEGATIVE, make the score value negative.
    # TODO make this line a little more nicer to read.
    sentiment_df = sentiment_df.withColumn('score',
                                            (when(sentiment_df.label == 'NEGATIVE', -sentiment_df.score)
                                            .otherwise(sentiment_df.score)))

    ## Old news weigh less
    # The older the news, the less important it is, scores are weighted
    # exponentially less every 3 hours from the most recent timestamp in the analysis window.

    # Calculate weight factor
    # Since we want the weight to be halved every 3 hours, the weight is basically
    # 1 / (2^h) where h is the hours away from max divided by 3 and rounded down [h = floor(diff_hour/3)]
    # e.g. 5 Hour difference from MAX timestamp means h = floor(5/3) and weight = 1/(2^1) = 1/2

    # Spark transformations needed:
    # 1. Get most latest (max) timestamp of news articles in each analysis window and stock
    # 2. Convert news_timestamp which is seconds from epoch to hour from epoch.
    # 3. Calculate the number of hour difference between the current row value and max value
    #    in terms of news_timestamp hours from epoch.
    # 4. Divide this difference by 3 and get the floor
    # 5. Calculate the weight which is 1/(2^h) where h = floor(diff_hour/3), h was calculated in step (3)
    # 6. Multiply the sentiment score by the weight to get the new time weighted score column


    # 1. Get max timestamp (epoch seconds) for each analysis window and stock ticker
    # https://stackoverflow.com/questions/49241264/
    # https://stackoverflow.com/questions/62863632/

    column_list = ['analysis_window', 'analysis_date', 't_symb']
    window_spec = Window.partitionBy([col(x) for x in column_list])
    sentiment_df = sentiment_df.withColumn('max_timestamp', spark_max(col('news_timestamp')).over(window_spec))

    # Convert from seconds from epoch to hour from epoch
    # Just divide the timestamp by 3600 seconds number of hours since epoch. 
    # (Worry about taking floor later)
    sentiment_df = sentiment_df.withColumn('max_timestamp_hours', sentiment_df.max_timestamp / 3600)
    sentiment_df = sentiment_df.drop('max_timestamp')


    # 2. Convert news_timestamp which is seconds from epoch to hour from epoch.
    sentiment_df = sentiment_df.withColumn('news_timestamp_hours', sentiment_df.news_timestamp/3600)
    sentiment_df = sentiment_df.drop('news_timestamp')

    # 3. Calculate the number of hour difference between the current row value and max value in terms of 
    #    news_timestamp hours from epoch.

    sentiment_df = sentiment_df.withColumn('diff_hours', sentiment_df.max_timestamp_hours - sentiment_df.news_timestamp_hours)
    sentiment_df = sentiment_df.drop('news_timestamp_hours') # don't need it anymore

    # 4. Divide this difference by 3 and get the floor
    staleness_period = 3
    sentiment_df = sentiment_df.withColumn('weight_denom_power', spark_floor(sentiment_df.diff_hours / staleness_period))
    sentiment_df = sentiment_df.drop('diff_hours')

    # Check for when difference is negative and throw and error or log it because something is wrong.
    # TODO maybe add this number to log file or throw error
    num_negatives = sentiment_df.filter(sentiment_df.weight_denom_power < 0).count()

    # 5. Calculate the weight which is 1/(2^h) where h = floor(diff_hour/3), h was calculated in step (3)
    sentiment_df = sentiment_df.withColumn('score_weight', 1/(2**sentiment_df.weight_denom_power))
    sentiment_df = sentiment_df.drop('weight_denom_power')

    # 6. Multiply the sentiment score by the weight to get the new time weighted score column
    sentiment_df = sentiment_df.withColumn('weighted_score', sentiment_df.score_weight * sentiment_df.score)

    ## Weighted average scores and change the labels

    # First we will sum all the weighted scores and divide it by the sum of
    # the score weights i.e. get a weighted average. This operation will be on
    # rows grouped by their respective analysis window and stock ticker symbol.

    # Then, instead of just **positive** and **negative**, we want one more
    # label called **uncertain** which is for scores less than 0.7
    # for either positive or negative.

    # Get sum of weights and sum of weighted scores
    sentiment_df = (sentiment_df.groupBy('analysis_window', 'analysis_date', 't_symb')
                                .agg(spark_sum('weighted_score').alias('sum_scores'),
                                    spark_sum('score_weight').alias('sum_weights'))
                )

    # Calculate final score for each stock
    sentiment_df = sentiment_df.withColumn('final_score', sentiment_df.sum_scores / sentiment_df.sum_weights)

    sentiment_df = (sentiment_df.withColumn('label',when(sentiment_df.final_score >= 0.5, 'POSITIVE')
                                                .when(sentiment_df.final_score <= -0.5, 'NEGATIVE')
                                                .otherwise('UNCERTAIN'))
                    )

    # Keep only entries that we need for the website
    sentiment_df = sentiment_df.select('analysis_window', 'analysis_date', 't_symb', 'label', 'final_score')

    # Cast float to Decimal
    # precision: the maximum total number of digits (default: 10)
    # scale: the number of digits on right side of dot. (default: 0)
    sentiment_df = sentiment_df.withColumn('final_score', sentiment_df.final_score.cast(DecimalType(precision=10, scale=8)))


    ### 5. Store the results into DynamoDB

    # Keep only entries that we need for the website
    # TODO add timestamp EST string
    sentiment_df = sentiment_df.select('analysis_window', 'analysis_date', 't_symb', 'label', 'final_score')

    # Cast float to Decimal
    # precision: the maximum total number of digits (default: 10)
    # scale: the number of digits on right side of dot. (default: 0)
    sentiment_df = sentiment_df.withColumn('final_score', sentiment_df.final_score.cast(DecimalType(precision=10, scale=8)))

    # Add the most recent API success timestamp for each stock, to be used for
    # "Last Updated at:" entry on the website because we want to reflect that the sentiments
    # are based on the time the data was pulled not the time the analysis pipeline finished.
    timestamp_groupby = news_df.select('analysis_window', 'analysis_date','t_symb','api_success_e_str').groupBy('analysis_window', 'analysis_date','t_symb')
    max_timestamp_df = timestamp_groupby.agg(spark_max('api_success_e_str').alias('api_success_e_str'))

    join_on_list = ["analysis_window","t_symb"]
    sentiment_df = sentiment_df.join(max_timestamp_df, join_on_list, "inner")

    # TODO use logger
    print('@@@ Executing Transformations')

    # Execute transformations and collect final dataframe in Driver
    results = sentiment_df.collect()

    results_dict_list = [row.asDict() for row in results]

    # TODO lots of error catches and logging needed!
    # TODO use logger
    print('Writing results to DynamoDB')
    dynamo.write_table(table_name=results_table, data_dict_list=results_dict_list)

    # TODO use logger
    print('@@@ DONE!')


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


def put_news_data(news_data: dict, table_name: str='test_data') -> dict:

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    response = table.put_item(Item=news_data)
    
    return response



# class SparkHelper():
#     def start_spark(cluster_url=None):
#         if cluster_url is None:
#             # Start Local Session
#             spark = (SparkSession.builder
#                                 .appName("SparkTest")  # Set app name
#                                 .master("local[2]")  # Run locally with 2 cores
#                                 .config("spark.driver.memory", "4g")
#                                 .config("spark.executor.memory", "3g")
#                                 .getOrCreate())
#         else:
#             spark = (SparkSession.builder
#                                 .appName("FaangSentimentSpark")  # Set app name
#                                 .master(cluster_url)
#                                 .config("spark.driver.memory", "4g")
#                                 .config("spark.executor.memory", "3g")
#                                 .getOrCreate())


class SentimentAnalysisPipeline:
    def __init__(self):
        # Note that the transformers pipelines can take list of strings as input
        # instead of just one string.

        # TODO uncomment this line when switching to summarizer method
        # self.summarizer_pipeline = pipeline("summarization")
        self.sentiment_pipeline = pipeline("sentiment-analysis")

        # multiprocessing core count heuristic from
        # comment in https://stackoverflow.com/questions/20886565/
        self.pool_cores = cpu_count()-1 or 1

    def strip_html(self, input_string):
        """
        Strips any HTML tags from a string
        """

        cleaned_text = BeautifulSoup(input_string).text

        return cleaned_text

    def strip_html_multi(self, input_string_list):

        # Make sure input is a list, or if it's one string
        # convert to list.
        if type(input_string_list) is not list:
            input_string_list = list(input_string_list)

        # TODO may want to initialize and destroy pool somwhere else
        # Or it might not matter too much given this function is only
        # called once per spark job / AWS EMR startup
        p = 2
        with Pool(processes=p) as pool:
            chunksize = 3
            no_html_text = pool.map(self.strip_html, input_string_list, chunksize)

        return no_html_text

    def raw_text_to_sentiment(self, input_string):
        """
        Takes a regular string and runs it through a pipeline to
        get the positive/negative label and the score.

        Pipeline consists of:
        (1) Removing HTML if present
        (2) Summarizing the news article
        (3) Calculating a sentiment score and label (Positive or Negative)

        Returns a list of dictionaries with label and score as keys.
        """

        # TODO Postpone using summarizer on full articles for now
        # until a better solution for token length too long issue is found.
        # TODO check for token length for summarizer
        # no_html_text = self.strip_html_multi(input_string)

        # news_summary = self.summarizer_pipeline(no_html_text,
        #                                        max_length=300,
        #                                        min_length=30)[0]['summary_text']
        # sentiment_scores = self.sentiment_pipeline(summary_news)

        # Temporary version expects news titles only
        label = self.sentiment_pipeline(input_string)[0]['label']
        score = self.sentiment_pipeline(input_string)[0]['score']

        return (label, score)


if __name__ == "__main__":
    main(sys.argv[1:])
