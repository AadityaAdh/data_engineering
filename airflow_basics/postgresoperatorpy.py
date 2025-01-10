from airflow import DAG
from datetime import datetime,timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


data=[
    {
        "name":"aaditya",
        "age":20
    },
    {
        "name":"ram",
        "age":30
    }
]

def insert_into_table():
    #insert garda chai hook use garni as hook lai loop ma chalauna milxa
    postgress_hook=PostgresHook(postgres_conn_id='test_connection')
    insert_query="""
        INSERT INTO tableone(name,age)
        VALUES(%s,%s)

    """
    for customers in data:
        postgress_hook.run(insert_query,parameters=(customers['name'],customers['age']))
        #note run yesto ma garni jaslae return kei gardaina
        #For non-returning queries like INSERT, UPDATE, DELETE, you can use run() directly.
        #so select ma garna mildaina
        #select garni ho vane get_records(my_sql_query)


def get_data_from_table():
    postgress_hook=PostgresHook(postgres_conn_id='test_connection')
    select_query="""
        select * from tableone where name=%s
    """
    results=postgress_hook.get_records(select_query,parameters=("aaditya",))
    #projects garda chai yo results lai push into xcomms
    print(results)


    


default_args={
    "owner":"aaditya",
    "retries":5,
    "retry_delay":timedelta(minutes=2)
}

with DAG(
    dag_id='postgres_dag',
    default_args=default_args,
    description="this is my postgrs dag",
    start_date=datetime(2025,1,10,4),#2025 1 mahina 10 gate dekhi jailae pani 4 bhajae
    schedule_interval="@daily"
) as dag:
    create_table_task=PostgresOperator(
        task_id="connectionid",
        postgres_conn_id="test_connection",
        sql="""
                create table if not exists tableone(
                name VARCHAR(255),
                age INTEGER,
                primary key (name,age)
                )

            """
        #primary key (name,age) yo vanae ko chai composite key :This means that the combination of name and age will be unique in the table. In other words, no two rows can have the same name and age combination.
        #name VARCHAR(255) PRIMARY KEY, yo vanae ko chai single primary key

    )
    #insertion task
    insertion_task=PythonOperator(#note yesto garda python operator ko use garya xa
        task_id="insertion_task",
        python_callable=insert_into_table
    )

    #so simple kuro matrai garnu xa vanae postgresoperator ko use garni
    #yedi select gare ra aako value xcom ma push garnu paryo vanae postgresoperator bata ta milae na ni
    #so basically always use postgreshook python operator 

    reading_task=PythonOperator(
        task_id="readingtask",
        python_callable=get_data_from_table
    )


    

    create_table_task>>reading_task




#read postgreshelp.txt