from airflow import DAG
from datetime import datetime,timedelta 
#timedelta chai delay ko lagi
from airflow.operators.bash import BashOperator

default_args={
    "owner":"aaditya",
    "retries":5,
    "retry_delay":timedelta(minutes=2)
}

with DAG(
    dag_id='our_first_dag_v4',
    default_args=default_args,
    description="this is my first dag",
    start_date=datetime(2025,1,10,4),#2025 1 mahina 10 gate dekhi jailae pani 4 bhajae
    schedule_interval="@daily"

) as dag:
    task1=BashOperator(
        task_id="first_task",
        bash_command="echo hello world"
    )

    task2=BashOperator(
        task_id="secound_task",
        bash_command="echo im the secound task and will run after task 1"
    )
    task3=BashOperator(
        task_id="third_task",
        bash_command="echo im the third task and run after task 1 and at same time as task 2"
    )

    #task1.set_downstream(task2)
    # can also be written as
    #task1>>task2  arrow bhanthanam na
    #task1.set_downstream(task3)
    #task1>>task3

    #as a whole garnu pare:
    #dependency vanae ko hirearchy in wich task have to be performed vanae ko kun order ma task perform garni vanae ko
    task1>>[task2,task3]

