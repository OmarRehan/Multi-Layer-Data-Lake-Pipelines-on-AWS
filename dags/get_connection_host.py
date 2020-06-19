from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
import datetime


def hello_world():
    from airflow.hooks.base_hook import BaseHook
    # print(BaseHook.get_connection('ssh_default').host)
    # print(BaseHook.get_connection('ssh_default').extra)
    #print(BaseHook.get_connection('ssh_default').extra_dejson)
    print(BaseHook.get_connection('ssh_default').login)


dag = DAG(
    'get_connection_host',
    description='DAG Desc',
    start_date=datetime.datetime(2020,5,1),
    schedule_interval=None
)

task = PythonOperator(
    task_id='hello_world',
    python_callable=hello_world,
    dag=dag)
