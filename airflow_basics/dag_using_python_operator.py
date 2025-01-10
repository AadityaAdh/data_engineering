from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
from py_task1 import task1 as t1
from py_task1 import get_name


default_args={
    "owner":"aaditya",
    "retries":5,
    "retry_delay":timedelta(minutes=5)
}


with DAG(
    dag_id="secound_dag_python_operator",
    default_args=default_args,
    description="this is dag using python operators",
    start_date=datetime(2025,1,10,5),
    schedule_interval="@daily"

) as dag:
    task1=PythonOperator(
        task_id="task1",
        python_callable=t1,
        #to pass arguments to t1 use op_kwargs
        op_kwargs={'name':'aaditya','age':20}#key args vayae ra dictionary pathako
    )
    task1
    
    