from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

"""
Old way of creation DAGs. Airflow 1.x.x available also in 2.x.x
"""


def test_one():
    meta_store_var = Variable.get("AIRFLOW_VAR_FOO_ONE")
    print(f"Execute test function one, meta_store_var var:{meta_store_var}")
    env_var = Variable.get("MY_VAR_ONE")
    print(f"Execute test function one, env var:{env_var}")


def test_two():
    print("Execute test function two")


with DAG(
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        "start_date": datetime(2022, 3, 23),
    },
    description='A simple tutorial DAG',
    dag_id="test_dag_example_id",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['example'],
) as dag:
    first_task_execute = PythonOperator(
        task_id="test_one",
        python_callable=test_one,
    )

    second_task_execute = PythonOperator(
        task_id="test_two",
        python_callable=test_two,
    )

first_task_execute >> second_task_execute
