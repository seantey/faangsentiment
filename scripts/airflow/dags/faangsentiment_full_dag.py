import boto3
import botocore
import json
import pytz

from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

# Useful XCom references:
# https://stackoverflow.com/questions/50093718/
# http://michal.karzynski.pl/blog/2017/03/19/developing-workflows-with-apache-airflow/


### Generate Analysis Window String
# This will be the first task in the dag and the generated string
# will be passed to other tasks using xcom.

def generate_analysis_window_str(**context):
    """
    Generate a string that looks like: YYYY-mm-dd_Hour=HH, e.g. 2020-07-30_Hour=13

    For this application, we will always be using time relative to Eastern Time.
    """
    timezone_str = 'US/Eastern'
    timezone = pytz.timezone(timezone_str)
    fmt = '%Y-%m-%d_Hour=%H'

    analysis_window_str = datetime.now().astimezone(timezone).strftime(fmt)

    # Push data with XCom
    task_instance = context['task_instance']
    task_instance.xcom_push('analysis_window', analysis_window_str)


### Lambda task related code
# Modified version of https://github.com/boto/boto3/issues/2424
def invoke_lambda(lambda_function_name, timeout_sec, **context):
    """
    Invokes the lambda function for given lambda function name
    """
    config = botocore.config.Config(
        read_timeout=timeout_sec,
        connect_timeout=timeout_sec,
        retries={"max_attempts": 0}  # Let the task fail and DAG will retry
    )

    session = boto3.Session()
    lambda_client = session.client("lambda", region_name='us-west-2', config=config)

    task_instance = context['task_instance']
    analysis_window = task_instance.xcom_pull(key='analysis_window',
                                              task_ids='gen_window_str')

    payload_dict = {'analysis_window': analysis_window}

    response = client.invoke(
        FunctionName=lambda_function_name,
        Payload=json.dumps(payload_dict),
        InvocationType="RequestResponse",
    )

    if response['StatusCode'] != 200:
        raise ValueError('Lambda function did not successfully execute.')

    data = response['Payload'].read()
    # TODO use logger, although airflow may or may not log this, or might need failure callback.
    # This in particular is not that necessary to log though.
    print(data)


### EMR task related code
emr_cluster_name = "faangsentiment_spark"
spark_script_s3_path = "s3://faangsentiment-scripts/emr/spark_process_sentiment.py"

# Create Job Flow Config (EMR Cluster Settings)
# The way I configure this is to first create an EMR Cluster from the AWS Web Console then export the CLI commands
# Then translate the CLI commands to a format compatible with the format from:
# https://docs.aws.amazon.com/emr/latest/APIReference/API_RunJobFlow.html#API_RunJobFlow_Examples
emr_settings = {"Name": emr_cluster_name,
                "LogUri": "s3://aws-logs-722339694227-us-west-2/elasticmapreduce/",
                "ReleaseLabel": "emr-5.30.1",
                "EbsRootVolumeSize": 10,
                "Instances": {
                    "InstanceGroups": [
                        {
                            "Name": "Master nodes",
                            "Market": "ON_DEMAND",
                            "InstanceRole": "MASTER",
                            "InstanceType": "m4.large",
                            "InstanceCount": 1,
                            "EbsConfiguration": {"EbsBlockDeviceConfigs":
                                [{"VolumeSpecification": {"SizeInGB": 32, "VolumeType": "gp2"}, "VolumesPerInstance": 1}]
                            }
                        },
                        {
                            "Name": "Slave nodes",
                            "Market": "ON_DEMAND",
                            "InstanceRole": "CORE",
                            "InstanceType": "m4.large",
                            "InstanceCount": 1,
                            "EbsConfiguration": {"EbsBlockDeviceConfigs":
                                [{"VolumeSpecification": {"SizeInGB": 32, "VolumeType": "gp2"}, "VolumesPerInstance": 1}]
                            }
                        }
                    ],
                    "Ec2KeyName": "faangsentiment",
                    "KeepJobFlowAliveWhenNoSteps": False,
                    'EmrManagedMasterSecurityGroup': 'sg-09b3c42a901efc6cb',
                    'EmrManagedSlaveSecurityGroup': 'sg-0f9d167925a286239',
                    "TerminationProtected": True
                },
                "Configurations": [
                    {"Classification": "spark-env",
                     "Properties": {},
                     "Configurations": [{"Classification": "export", "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}}]
                     },
                    {"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}}
                ],
                "BootstrapActions": [
                    {
                        'Name': 'Load PySpark Script',
                        'ScriptBootstrapAction': {
                            'Path': 's3://faangsentiment-scripts/emr/emr_bootstrap_install.sh'
                        }
                    }
                ],
                "Applications": [
                    {"Name": "Spark"}
                ],
                "VisibleToAllUsers": True,
                "ScaleDownBehavior": "TERMINATE_AT_TASK_COMPLETION",
                "JobFlowRole": "EMR_EC2_DefaultRole",
                "ServiceRole": "EMR_DefaultRole",
                "AutoScalingRole": "EMR_AutoScaling_DefaultRole"
                }


def run_emr_job(**context):
    """
        Launches an EMR clusters, runs a spark script, terminates when done.
    """
    client = boto3.client('emr', region_name='us-west-2')
    response_create = client.run_job_flow(**emr_settings)
    jobflow_id = response_create['JobFlowId']

    task_instance = context['task_instance']
    analysis_window = task_instance.xcom_pull(key='analysis_window',
                                              task_ids='gen_window_str')

    # EMR configuration to fetch and run PySpark code.
    spark_step_config = [
        {
            "Name": "PySpark application",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": ["spark-submit", "--deploy-mode", "cluster",
                         spark_script_s3_path, analysis_window]
            }
        }
    ]
    response_add = client.add_job_flow_steps(JobFlowId=jobflow_id, Steps=spark_step_config)
    # In our case we usually only have on step which is running the spark script.
    step_id = response_add['StepIds'][0]

    # Add waiter which basically blocks the function from continuing until Spark script finishes.
    # Ref: https://stackoverflow.com/questions/44798259/

    waiter = client.get_waiter("step_complete")
    waiter.wait(
        ClusterId=jobflow_id,
        StepId=step_id,
        WaiterConfig={
            # Wait 30 seconds before polling again.
            "Delay": 30,
            # Poll 50 times for a total of 1500 seconds or 25 minutes.
            # Make sure the total poll time is enough for the cluster to finish its job.
            "MaxAttempts": 50
        }
    )


### Dag related code

# Dag default settings
default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@faangsentiment.app'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=35)
}

# Initialize DAG
# Full pipeline consists of 4 stages:
# (1) Generate analysis window string which is required by the other tasks.
# (2) Pull data from news apis by invoking AWS Lambda function which stores the data in DynamoDB
# (3) Start EMR Cluster which pulls and processes the news data then stores it in a different
# DynamoDB table and the configuration will terminate the cluster once the SparkJob is complete.
# (4) Fetch the sentiment analysis results then process and subsets the data to a JSON format
# compatible with the faangsentiment.app website and stores it in another table which is
# pulled by the website javascript code.
with DAG('faaangsentiment_full_pipeline',
         default_args=default_dag_args,
         description='FaangSentiment Full DAG',
         schedule_interval='0 * * * *',  # Start at beginning of every hour i.e. every minute zero
         catchup=False,  # Do not backfill when starting DAG
         max_active_runs=2
         ) as dag:

    # (1) Generate analysis window string which is required by the other tasks.
    gen_analysis_window_task = PythonOperator(
        task_id='gen_window_str',
        python_callable=generate_analysis_window_str,
        provide_context=True  # Needed so that xcom values are visible.
    )

    # (2) Pull data from news apis by invoking AWS Lambda function which stores the data in DynamoDB
    fetch_news_task = PythonOperator(
        task_id='fetch_news',
        python_callable=invoke_lambda,
        op_kwargs={'lambda_function_name': 'faangsentiment_news', 'timeout_sec': 500},
        provide_context=True  # Needed so that xcom values are visible.
    )

    # (3) Start EMR Cluster which pulls and processes the news data then stores it in a different
    # DynamoDB table and the configuration will terminate the cluster once the SparkJob is complete.

    calculate_sentiment_task = PythonOperator(
        task_id='calculate_sentiment',
        python_callable=run_emr_job,
        provide_context=True
    )

    # (4) Fetch the sentiment analysis results then process and subsets the data to a JSON format
    # compatible with the faangsentiment.app website and stores it in another table which is
    # pulled by the website javascript code.

    convert_to_json_task = PythonOperator(
        task_id='convert_to_json',
        python_callable=invoke_lambda,
        op_kwargs={'lambda_function_name': 'results_to_json', 'timeout_sec': 500},
        provide_context=True
    )

    gen_analysis_window_task >> fetch_news_task >> calculate_sentiment_task >> convert_to_json_task
