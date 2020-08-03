import boto3
import pytz
from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


def generate_analysis_window_str(**context):
    """
    Fake version of the original function.
    Generate a string that looks like: YYYY-mm-dd_Hour=HH, e.g. 2020-07-30_Hour=13

    For this application, we will always be using time relative to Eastern Time.
    """
    # timezone_str = 'US/Eastern'
    # timezone = pytz.timezone(timezone_str)
    # fmt = '%Y-%m-%d_Hour=%H'

    # analysis_window_str = datetime.now().astimezone(timezone).strftime(fmt)

    analysis_window_str = 'TEST WINDOW'

    # Push data with XCom
    task_instance = context['task_instance']
    task_instance.xcom_push('analysis_window', analysis_window_str)


def print_args_and_context(item1, item2, **context):
    log_string = f'Item1 = {item1}, Item2 = {item2}, Context = {context}'
    print(f'Item1 = {item1}')
    print(f'Item2 = {item2}')
    print(f'Context = {context}')

    if item1 != 'faangsentiment_news' or item2 != 500:
        raise ValueError('Item contents are wrong')

    task_instance = context['task_instance']
    analysis_window = task_instance.xcom_pull(key='analysis_window',
                                              task_ids='gen_window_str')

    if analysis_window != 'TEST WINDOW':
        raise ValueError('XCOM value is wrong')

    # Reverse test, force it to throw error
    # if item1 == 'faangsentiment_news' and item2 == 500 and analysis_window == 'TEST WINDOW':
    #     raise ValueError('It works!')

    return log_string

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

with DAG('context_test',
         default_args=default_dag_args,
         description='FaangSentiment Full DAG',
         schedule_interval='0 * * * *',  # Start at beginning of every hour i.e. every minute zero
         catchup=False,  # Do not backfill when starting DAG
         max_active_runs=2
         ) as dag:

    gen_analysis_window_task = PythonOperator(
        task_id='gen_window_str',
        python_callable=generate_analysis_window_str,
        provide_context=True  # Needed so that xcom values are visible.
    )

    print_args_task = PythonOperator(
        task_id='print_args_test',
        python_callable=print_args_and_context,
        op_kwargs={'item1': 'faangsentiment_news', 'item2': 500},
        provide_context=True  # Needed so that xcom values are visible.
    )

    gen_analysis_window_task >> print_args_task