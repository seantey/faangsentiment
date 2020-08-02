from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
)

from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

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
    'execution_timeout': timedelta(minutes=2)
}

# Create Job Flow Config references
# The way I configure this is to first create an EMR Cluster from the AWS Web Console then export the CLI commands
# Then translate the CLI commands to a format compatible with the format from:
# https://docs.aws.amazon.com/emr/latest/APIReference/API_RunJobFlow.html#API_RunJobFlow_Examples
# Also useful reference: https://github.com/popoaq/airflow-dags/blob/dd929644e35afe07f2633daf98c7b1026da1390a/dags/finance.py
emr_settings = {"Name": "faangsentiment_spark_EMR_TEST",
                "LogUri": "s3://aws-logs-722339694227-us-west-2/elasticmapreduce/",
                "ReleaseLabel": "emr-5.30.1",
                "EbsRootVolumeSize": 10,
                "Instances": {
                    "InstanceGroups": [
                        {
                            "Name": "Master nodes",
                            "Market": "ON_DEMAND",
                            "InstanceRole": "MASTER",
                            "InstanceGroupType":"MASTER",
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
                "AutoScalingRole": "EMR_AutoScaling_DefaultRole",
                }


# Initialize DAG
with DAG('lambda_test',
         default_args=default_args_dag,
         description='Simple EMR test DAG',
         schedule_interval=timedelta(hours=1),
         catchup=False,
         max_active_runs=1
         ) as dag:

    create_job_flow = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
        job_flow_overrides=default_emr_settings
    )

    spark_step_config = [
        {
            "Name": "PySpark application",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": ["spark-submit", "--deploy-mode", "cluster",
                         "s3://faangsentiment-scripts/emr/test_spark_emr.py"]
            }
        }
    ]

    spark_step = EmrAddStepsOperator(
        task_id='add_spark_step',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=spark_step_config
    )

    watch_prev_step_task = EmrStepSensor(
        task_id='watch_prev_step',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull('add_step', key='return_value')[0] }}",
        aws_conn_id='aws_default'
    )

    terminate_job_flow_task = EmrTerminateJobFlowOperator(
        task_id='terminate_job_flow',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
        trigger_rule="all_done"
    )

    create_job_flow_task >> add_step_task
    add_step_task >> watch_prev_step_task
    watch_prev_step_task >> terminate_job_flow_task

# For reference, original CLI export output
# aws emr create-cluster
# --termination-protected 
# --applications Name=Hadoop Name=Spark 
# --ec2-attributes 
# {"KeyName":"faangsentiment",
# "InstanceProfile":"EMR_EC2_DefaultRole",
# "SubnetId":"subnet-1760af6f",
# "EmrManagedSlaveSecurityGroup":"sg-09b3c42a901efc6cb",
# "EmrManagedMasterSecurityGroup":"sg-0f9d167925a286239"} 

# --release-label emr-5.30.1 --log-uri 's3n://aws-logs-722339694227-us-west-2/elasticmapreduce/' 

# --steps
# [
#     {"Args":["spark-submit","--deploy-mode","cluster","s3://faangsentiment-scripts/emr/test_spark_emr.py"],
#     "Type":"CUSTOM_JAR",
#     "ActionOnFailure":"TERMINATE_CLUSTER",
#     "Jar":"command-runner.jar",
#     "Properties":"","Name":"Spark application"}
# ]

# --instance-groups 
# [
#     {"InstanceCount":1,
#         "EbsConfiguration":{"EbsBlockDeviceConfigs":
#                                 [{"VolumeSpecification": {"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]
#                             },
#         "InstanceGroupType":"MASTER",
#         "InstanceType":"m4.large","Name":"Master - 1"
#     },
#     {"InstanceCount":1,
#         "EbsConfiguration":{"EbsBlockDeviceConfigs":
#                                 [{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]
#                             },
#         "InstanceGroupType":"CORE",
#         "InstanceType":"m4.large","Name":"Core - 2"}
# ]

# --configurations 
# [
#     {"Classification":"spark-env",
#     "Properties":{},
#     "Configurations":[{"Classification":"export","Properties":{"PYSPARK_PYTHON":"/usr/bin/python3"}}]
#     },
#     {"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"}}]

# @@@@@@@@@@@@@@@@@@@--auto-terminate 

# --auto-scaling-role EMR_AutoScaling_DefaultRole 

# --bootstrap-actions '[{"Path":"s3://faangsentiment-scripts/emr/emr_bootstrap_install.sh","Name":"Custom action"}]' 

# --ebs-root-volume-size 10 

# --service-role EMR_DefaultRole 

# --enable-debugging 

# --name 'EMR_Test' 

# --scale-down-behavior TERMINATE_AT_TASK_COMPLETION 

# @@@@@@@@@@@@@@--region us-west-2
