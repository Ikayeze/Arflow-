from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


# first we write our python logic

# define a function

def python_first_function():
    print("Hi ikay, im a python function called by airflow")

def python_second_function():
    p = []
    for i in range(20):
        p.append(i)
    return p
#create the DAG which calls the python logic that we created

default_dag_args = {
    'start_date':datetime(2023,1,1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay':timedelta(minutes=5),
    'project_id': 1
}


with DAG("first_python_dag", schedule_interval = '@daily', catchup=False, default_args =default_dag_args ) as dag_python:

    #here we define our tasks
    task_0 = PythonOperator(task_id = "first_python_task", python_callable = python_first_function)

with DAG("loop_dag", schedule_interval = None, default_args = default_dag_args) as second_python_dag:
    task_1 = PythonOperator(task_id = 'for_loop', python_callable = python_second_function)