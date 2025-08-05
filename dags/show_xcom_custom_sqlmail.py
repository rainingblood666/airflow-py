from airflow import DAG
from mssqlmail_operator import MsSQLMailOperator
from getenv_sql_operator import GetEnvSQLOperator  # Assuming GetEnvSQLOperator is defined in another file
from datetime import datetime, timedelta
env ='prod'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'show_xcom_custom_sqlmail',
    default_args=default_args,
    description='demionstration of custom SQL mail operator',
     schedule=timedelta(days=1),
)


get_env_task = GetEnvSQLOperator(
    task_id="get_env_task",
    mssql_conn_id='my_mssql',
    dag=dag)

send_mail_Person_task= MsSQLMailOperator(task_id="send_mail_Person_task",
                                   env='{{ task_instance.xcom_pull("get_env_task", key="return_value") }}',
                                   mssql_conn_id='my_mssql',
                                   table="[AdventureWorks2022].[Person].[Person]",
                                   columns='BusinessEntityID, PersonType, NameStyle, Title, FirstName, MiddleName, LastName,  EmailPromotion')
send_mail_Account_task = MsSQLMailOperator(task_id="send_mail_Account_task",
                                   env="{{ task_instance.xcom_pull('get_env_task', key='return_value') }}",
                                   mssql_conn_id='my_mssql',
                                   table="[AdventureWorksDW2022].[dbo].[DimAccount]",
                                   columns='[AccountDescription],AccountType,Operator,[ValueType] ')
get_env_task >> [send_mail_Person_task,send_mail_Account_task] 