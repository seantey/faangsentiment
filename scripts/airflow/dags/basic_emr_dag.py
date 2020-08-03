import boto3
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

# Default ARGS passed into all operators
default_args_dag = {
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

emr_cluster_name = "faangsentiment_spark_EMR_TEST"
spark_script_s3_path = "s3://faangsentiment-scripts/emr/test_spark_emr.py"

# Create Job Flow Config references
# The way I configure this is to first create an EMR Cluster from the AWS Web Console then export the CLI commands
# Then translate the CLI commands to a format compatible with the format from:
# https://docs.aws.amazon.com/emr/latest/APIReference/API_RunJobFlow.html#API_RunJobFlow_Examples
# Also useful reference: https://github.com/popoaq/airflow-dags/blob/dd929644e35afe07f2633daf98c7b1026da1390a/dags/finance.py
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
                    "KeepJobFlowAliveWhenNoSteps": True,
                    'EmrManagedMasterSecurityGroup': 'sg-09b3c42a901efc6cb',
                    'EmrManagedSlaveSecurityGroup': 'sg-0f9d167925a286239',
                    "TerminationProtected": True,
                    # If specific availability zone is preferred.
                    # 'Placement': {
                    #     'AvailabilityZone': 'us-west-2c',
                    # },
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
    client = boto3.client('emr')
    response = client.run_job_flow(**emr_settings)
    jobflow_id = response['JobFlowId']
    client.add_job_flow_steps(JobFlowId=jobflow_id, Steps=spark_step_config)

# Initialize DAG
with DAG('emr_test',
         default_args=default_args_dag,
         description='Simple EMR test DAG',
         schedule_interval=timedelta(hours=1),
         catchup=False,
         max_active_runs=1
         ) as dag:

    calculate_sentiment_task = PythonOperator(
        task_id='calculate_sentiment',
        python_callable=run_emr_job
    )

    calculate_sentiment_task
