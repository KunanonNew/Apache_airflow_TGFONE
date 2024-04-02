from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'test_conn',
    description='A simple DAG to test PostgreSQL connection',
    schedule_interval=None,
    tags=["demo"]
):

    check_connection = PostgresOperator(
        task_id='check_connection',
        postgres_conn_id='my_postgres_conn',
        sql='SELECT 1',
    )

check_connection
