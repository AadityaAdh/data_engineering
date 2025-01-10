from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator

#yedi kunai pani function xa tesma return value xa 
#aaba yo function ta yeuta task ma hunxa
#so kunai pani return value of a function in one task chai use garnu paryo aarko task or function ma vanae x comms use hunxa



'''
function ma return garya xa vnae xcomms ma aafai gayae ra push vaidinxa
aani tyo value ko key vanae ko chai return_value hunxa

yo value access garna jun function le parxa tesma ti vanae ra aargument pathauni
ti=task instance object

yo objcet ko dherai method xan
 aaba tyo value pull garna ko lagi chai ti.xcom_pull(key,task id)

 
 https://www.youtube.com/watch?v=8veO7-SN5ZY

 yo video herni bhujinxa



'''

# https://www.youtube.com/watch?v=8veO7-SN5ZY

def pushusingreturn():
    #return garyo vanae aafai push hunxa using key return_value
    return "aaditya"

def returnlegarekopushpull(ti,age):
        name=ti.xcom_pull(task_ids="pr")#yesma chai key vani ranu pardaina task matrai vanda hunxa ooslae kei dya xaina vanae return_value key vanae ra bhujxa
        print(f'my name is {name} and my age is{age} ')





def aafaipush(ti):
    #aafai push garna ta chainxa ti
    ti.xcom_push(key="name",value="aafaipush")



def greet(age,ti):
    name=ti.xcom_pull(key="name",task_ids="ap")#first task le push gare ko name:"..." ko value pull gare ko
    print(f'my name is {name} and my age is{age}. ')




default_args={
    "owner":"aaditya",
    "retries":5,
    "retry_delay":timedelta(minutes=5)
}


with DAG(
    dag_id="use_xcomms",
    default_args=default_args,
    description="this is dag using python operators",
    start_date=datetime(2025,1,10,5),
    schedule_interval="@daily"

) as dag:
    task1=PythonOperator(
         task_id="pr",
         python_callable=pushusingreturn,
    )
    task2=PythonOperator(
         task_id="rp",
         python_callable=returnlegarekopushpull,
            op_kwargs={"age":20}#note yaa bata ti pathaunu chai pardaina


         
    )

    task3=PythonOperator(
         task_id="ap",
         python_callable=aafaipush,
    )
    task4=PythonOperator(
         task_id="g",
         python_callable=greet,
            op_kwargs={"age":20}#note yaa bata ti pathaunu chai pardaina


         
    )

    task1>>task2>>task3>>task4


    #so simply vannu parda yeuta task ko output aarko task ma use garnu paryo vanae yo use garni
    
    
    
    