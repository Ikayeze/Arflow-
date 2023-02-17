from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_dag_args = {'start_date': datetime(2023,1,1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id':1}

with DAG('first_dag',schedule_interval=None,default_args=default_dag_args)as dag:
    task_0= BashOperator(task_id='bash_task',bash_command="echo 'command executed from bash operator'")
    task_1= BashOperator(task_id='bash_task_move_data',bash_command='cp C:\\Users\\HP\\Documents\\DATA CENTER\\data_lake\\dataset_raw.txt  \\C:\\Users\\HP\\Documents\\DATA CENTER\\clean_data')
    task_2= BashOperator(task_id='bash_task_delete', bash_command= 'rm C:\\Users\\HP\\Documents\\DATA CENTER\\data_lake ')


task_0 >> task_1 >> task_2




