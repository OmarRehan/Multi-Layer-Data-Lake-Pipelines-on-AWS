from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
import datetime


def hello_world():
    print('Hello World')


dag = DAG(
    'python-print',
    description='DAG Desc',
    start_date=datetime.datetime(2020,5,1),
    schedule_interval=None
)

task = PythonOperator(
    task_id='hello_world',
    python_callable=hello_world,
    dag=dag)

emr_connect1 = SSHHook(ssh_conn_id="ssh_default")

emr_connect2 = SSHHook(remote_host='172.31.19.158',key_file='/home/admin_123/Desktop/Main/Configs/spark-cluster-key.pem')

emr_copy_s1 = SSHOperator(
    task_id='Task_id',
    ssh_hook=emr_connect1,
    command="echo 'hello world'",
    dag=dag,
)


emr_copy_s2 = SSHOperator(
    task_id='Task_id2',
    ssh_hook=emr_connect2,
    command="echo 'hello world'",
    dag=dag,
)
