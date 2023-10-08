"""A simple DAG that prints Hello, World!""" ""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def print_hello():
    """Prints Hello, World!"""
    print("Hello, World!")


dag = DAG(
    "hello_world",
    description="Simple tutorial DAG",
    schedule_interval="0 12 * * *",
    start_date=datetime(2023, 10, 6),
    catchup=False,
)

hello_operator = PythonOperator(
    task_id="hello_task", python_callable=print_hello, dag=dag
)
