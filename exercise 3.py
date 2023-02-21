import requests
import time
from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator



default_dag_args= {
    'start_date': datetime(2023,1,1),
    'email_on_failure': False,
    'email-on_retry': False,
    'retries': 1,
    'retry_delay':timedelta(minutes=2),
    'project_id':1
}



def get_data(**kwargs):
    ticker = kwargs['ticker']
    api_key = 'JJ2EJSN9PTN2RRNL'
   
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=' + ticker +'&apikey='+api_key
    r = requests.get(url)
    path = 'C:/Users/HP/Documents/DATA_CENTER/data_lake/'
    data = r.json()
    try:
        with open(path + "stock_market_raw_data" + ticker + "_" + str(time.time()), "w") as outfile:
            json.dump(data, outfile)
    except:
        return 'nuone'
        
        

with DAG("market_data_alphavantage2", schedule_interval = None, default_args = default_dag_args) as get_IBM_weekly:
    task_0 = PythonOperator(task_id = 'get_IBM_market', python_callable = get_data, op_kwargs = {'ticker':"IBM"})