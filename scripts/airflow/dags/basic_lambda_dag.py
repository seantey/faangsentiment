from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Just a test. TODO remove later
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator

import boto3

# Default ARGS passed into all operators
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@faangsentiment.app'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'execution_timeout': timedelta(minutes=2)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


def basic_lambda_test():
    lambda_client = boto3.client('lambda', region_name='us-west-2')
    response = lambda_client.invoke(
            FunctionName='airflow_test',
            InvocationType='RequestResponse'
    )
    data = response['Payload'].read()
    print(data)


# Initialize DAG
with DAG('lambda_test',
         default_args=default_args,
         description='Simple lambda hook test DAG',
         schedule_interval=timedelta(minutes=2),
         catchup=False,
         max_active_runs=1
         ) as dag:


    lambda_test = PythonOperator(
        task_id='lambda_test',
        python_callable=basic_lambda_test
    )

    # dummy_start_task >> lambda_test
    lambda_test
