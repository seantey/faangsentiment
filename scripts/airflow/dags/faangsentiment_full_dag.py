import boto3
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


### Lambda task related code

# News API Task

def invoke_lambda(**func_args):
    """
    Invokes the lambda function for given lambda function name
    """
    lambda_client = boto3.client('lambda', region_name='us-west-2')
    response = lambda_client.invoke(
            FunctionName=func_args['lambda_func_name'],
            InvocationType='RequestResponse'
    )
    data = response['Payload'].read()

    # TODO use logger, although airflow might not log this.
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


# EMR configuration to fetch and run PySpark code.
spark_step_config = [
    {
        "Name": "PySpark application",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit", "--deploy-mode", "cluster",
                        spark_script_s3_path]
        }
    }
]


def run_emr_job():
    """
        Launches an EMR clusters, runs a spark script, terminates when done.
    """
    client = boto3.client('emr', region_name='us-west-2')
    response = client.run_job_flow(**emr_settings)
    jobflow_id = response['JobFlowId']
    client.add_job_flow_steps(JobFlowId=jobflow_id, Steps=spark_step_config)


### Dag related code

# Dag default settings
default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@faangsentiment.app'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=35)
}

# Initialize DAG
# Full pipeline consists of 3 stages:
# (1) Pull data from news apis by invoking AWS Lambda function which stores the data in DynamoDB
# (2) Start EMR Cluster which pulls and processes the news data then stores it in a different
# DynamoDB table and the configuration will terminate the cluster once the SparkJob is complete.
# (3) Fetch the sentiment analysis results then process and subsets the data to a JSON format
# compatible with the faangsentiment.app website and stores it in another table which is
# pulled by the website javascript code.
with DAG('faaangsentiment_full_pipeline',
         default_args=default_dag_args,
         description='FaangSentiment Full DAG',
         schedule_interval='0 * * * *',  # Start at beginning of every hour i.e. every minute zero
         catchup=False,  # Do not backfill when starting DAG
         max_active_runs=1
         ) as dag:

    # (1) Pull data from news apis by invoking AWS Lambda function which stores the data in DynamoDB
    fetch_news_task = PythonOperator(
        task_id='fetch_news',
        python_callable=invoke_lambda,
        op_kwargs={'lambda_func_name': 'faangsentiment_news'}
    )

    # (2) Start EMR Cluster which pulls and processes the news data then stores it in a different
    # DynamoDB table and the configuration will terminate the cluster once the SparkJob is complete.

    calculate_sentiment_task = PythonOperator(
        task_id='calculate_sentiment',
        python_callable=run_emr_job
    )

    # (3) Fetch the sentiment analysis results then process and subsets the data to a JSON format
    # compatible with the faangsentiment.app website and stores it in another table which is
    # pulled by the website javascript code.

    convert_to_json_task = PythonOperator(
        task_id='convert_to_json',
        python_callable=invoke_lambda,
        op_kwargs={'lambda_func_name': 'results_to_json'}
    )

    fetch_news_task >> calculate_sentiment_task >> convert_to_json_task
